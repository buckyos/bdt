// Copyright (c) 2016-2018, BuckyCloud, Inc. and other BDT contributors.
// The BDT project is supported by the GeekChain Foundation.
// All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//     * Redistributions of source code must retain the above copyright
//       notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of the BDT nor the
//       names of its contributors may be used to endorse or promote products
//       derived from this software without specific prior written permission.

// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
// ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
// WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY
// DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
// (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
// LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
// ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
// SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

'use strict';

const {Config, RandomGenerator, EndPoint} = require('./util.js');
const DHTPackage = require('./packages/package.js');
const Base = require('../base/base.js');
const Peer = require('./peer.js');
const assert = require('assert');
const BaseUtil = require('../base/util.js');
const TimeHelper = BaseUtil.TimeHelper;

const LOG_WARN = Base.BX_WARN;
const LOG_INFO = Base.BX_INFO;

const RouteTableConfig = Config.RouteTable;

class RouteTable {
    constructor({taskExecutor, bucket, packageFactory, packageSender}) {
        this.m_taskExecutor = taskExecutor;
        this.m_bucket = bucket;
        this.m_packageFactory = packageFactory;
        this.m_packageSender = packageSender;
        this.m_nextExpandTime = 0;
        this.m_peerCountLastExpand = -128; // 最后一次扩充路由表时的peer数，初始置负数，保证第一次及时更新扩充路由表
        this.m_time4LastPackageFromInternet = 0;
        this.m_lastPingTime = 0;
    }

    refresh() {
        this._expand();
        this._pingAllTimeoutPeers();
    }

    ping(peer) {
        let pingPackage = this.m_packageFactory.createPackage(DHTPackage.CommandType.PING_REQ);
        this.m_packageSender.sendPackage(peer, pingPackage, false, RouteTableConfig.PingIntervalMS.Retry);
    }

    onRecvPackage(cmdPackage, socket, remotePeer, remoteAddress) {
        if (!EndPoint.isNAT(remoteAddress)) {
            this.m_time4LastPackageFromInternet = TimeHelper.uptimeMS();
        }
    }

    static _randomPeerid() {
        let length = RandomGenerator.integer(16, 4);
        return RandomGenerator.string(length);
    }

    _expand() {
        let now = TimeHelper.uptimeMS();
        let maxInterval = this._maxExpandInterval();

        if (this.m_nextExpandTime <= now) {
            this.m_nextExpandTime = Infinity;
            this.m_peerCountLastExpand = this.m_bucket.peerCount;
            this.m_taskExecutor.findPeer(RouteTable._randomPeerid(), false,
                () => this.m_nextExpandTime = TimeHelper.uptimeMS() + this._maxExpandInterval()
            );
        }
    }

    // 查询周期随peer数量增多变长
    _maxExpandInterval() {
        let peerCount = this.m_bucket.peerCount;
        return RouteTableConfig.ExpandIntervalMS.dynamic(peerCount, peerCount - this.m_peerCountLastExpand);
    }

    // 受限/对称NAT应该对较多节点进行高频的ping维持穿透；
    // 全锥形NAT，只要近期有对外recv包即可维持穿透，维持相对长时间的ping间隔测试其他在线状态
    // 公网Peer不需要维持穿透状态，只要定时测试其他peer在线状态即可
    _pingAllTimeoutPeers() {
        let now = TimeHelper.uptimeMS();
        let pingPackage = null;
        let pingInterval4NATType = RouteTableConfig.PingIntervalMS.Max; // 按本地peer网络环境定制ping频率，置0表示要按需求动态调整
        let localPeer = this.m_bucket.localPeer;
        switch (localPeer.natType) {
            case Peer.NAT_TYPE.unknown: // fallthrough
            case Peer.NAT_TYPE.restrictedNAT: // fallthrough
            case Peer.NAT_TYPE.symmetricNAT: // fallthrough
                pingInterval4NATType = 0;
                break;
            case Peer.NAT_TYPE.NAT:
            case Peer.NAT_TYPE.internet:
            if (now - this.m_time4LastPackageFromInternet > RouteTableConfig.PingIntervalMS.Min) {
                    pingInterval4NATType = 0; // 很长时间没收到来自公网的包了，按受限NAT频率ping一遍
                }
                break;
            default:
                break;
        }

        let pingCount = 0;
        if (this.m_lastPingTime > now) {
            this.m_lastPingTime = 0;
        }
        let maxPingCount = this.m_peerCountLastExpand * (now - this.m_lastPingTime) / RouteTableConfig.PingIntervalMS.Min;
        if (maxPingCount < 1) {
            return;
        }

        // 近期离线
        const isOfflineRecently = (peer) => {
            return !peer.isOnline(this.m_bucket.TIMEOUT_MS) // 不在线
                && now - getLastTransTime(peer).recv.all < this.m_bucket.TIMEOUT_MS + RouteTableConfig.PingIntervalMS.Retry * 3;
        }

        const shoudRetry = (peer, bucketDistRank) => {
            let pingInterval = pingInterval4NATType || RouteTableConfig.PingIntervalMS.dynamic(bucketDistRank);
            // 在一个ping周期略长的时间内没有收到包，很可能是ping包丢失；
            // 近期离线的也应该retry，可能还能救回
            const lastRecvTime = getLastTransTime(peer).recv.all;
            return (now - lastRecvTime >= pingInterval && now - lastRecvTime < pingInterval + RouteTableConfig.PingIntervalMS.Retry * 3) ||
                isOfflineRecently(peer);
        }

        let lastRank = {
            rank: -1,
            recentRecvPeer: null,
            recentRecvTime: 0,
        };

        const ping = (peer) => {
            if (!pingPackage) {
                pingPackage = this.m_packageFactory.createPackage(DHTPackage.CommandType.PING_REQ);
            }

            const lastTransTime = getLastTransTime(peer);
            
            // 太长时间没有收到UDP包，也没有发出UDP包，忽略之前发送路由缓存，对该peer所有地址发送一遍
            let ignoreCache = (now - localPeer.lastRecvTimeUDP >= RouteTableConfig.PingIntervalMS.Min &&
                                now - localPeer.lastSendTimeUDP >= RouteTableConfig.PingIntervalMS.Retry) || 
                            (now - lastTransTime.recv.udp >= RouteTableConfig.PingIntervalMS.Max &&
                                now - lastTransTime.send.udp >= RouteTableConfig.PingIntervalMS.Retry);
            this.m_packageSender.sendPackage(peer, pingPackage, ignoreCache, RouteTableConfig.PingIntervalMS.Retry);
            pingCount++;
        }

        // 启动多个DHT栈的情况下，同一个PEER可能加入多个DHT网络，ping的频率应该共享
        const getLastTransTime = peer => {
            if (peer.__transFrequnence && now - peer.__transFrequnence.updateTime < 5000) {
                return peer.__transFrequnence;
            }

            let udpEPList = [];
            let tcpEPList = [];
            peer.eplist.forEach(ep => {
                const addr = EndPoint.toAddress(ep);
                if (!EndPoint.isNAT(addr)) {
                    if (addr.protocol === EndPoint.PROTOCOL.tcp) {
                        tcpEPList.push(ep);
                    } else {
                        udpEPList.push(ep);
                    }
                }
            });

            const lastRecvTime = {
                udp: this.m_packageSender.mixSocket.getLastRecvTime(udpEPList),
                tcp: this.m_packageSender.mixSocket.getLastRecvTime(tcpEPList),
            };
            const lastSendTime = {
                udp: this.m_packageSender.mixSocket.getLastSendTime(udpEPList),
                tcp: this.m_packageSender.mixSocket.getLastSendTime(tcpEPList),
            };

            peer.__transFrequnence = {
                recv: {
                    udp: Math.max(peer.lastRecvTimeUDP, lastRecvTime.udp),
                    all: Math.max(peer.lastRecvTime, lastRecvTime.udp, lastRecvTime.tcp),
                },
                send: {
                    udp: Math.max(peer.lastSendTimeUDP, lastSendTime.udp),
                    all: Math.max(peer.lastSendTime, lastSendTime.udp, lastSendTime.tcp),
                },

                updateTime: now,
            };
            return peer.__transFrequnence;
        }

        this.m_bucket.forEachPeer(peer => {
            if (pingCount >= maxPingCount || peer === localPeer) {
                return;
            }

            // 按距离确定ping时间间隔，刚刚超时的peer可能只是丢包，最近抓紧时间重试几次
            // 每个距离等级上都保留一个相对高频的ping，提高远距离peer之间的连通率
            let subBucket = this.m_bucket.findOwnerSubBucketOfPeer(peer);
            if (!subBucket) {
                assert(!this.m_bucket.findPeer(peer.peerid));
                return;
            }

            const lastTransTime = getLastTransTime(peer);
            if (subBucket.distRank != lastRank.rank) {
                const pingInterval = (pingInterval4NATType || RouteTableConfig.PingIntervalMS.Min);
                if (lastRank.recentRecvPeer &&
                    now - lastRank.recentRecvTime > pingInterval &&
                    now - getLastTransTime(lastRank.recentRecvPeer).send.all > pingInterval) {
                    ping(lastRank.recentRecvPeer);
                }
                lastRank.rank = subBucket.distRank;
                lastRank.recentRecvPeer = peer;
                lastRank.recentRecvTime = lastTransTime.recv.all;
            } else if (lastTransTime.recv.all > lastRank.recentRecvTime) {
                lastRank.recentRecvTime = lastTransTime.recv.all;
                lastRank.recentRecvPeer = peer;
            }

            const pingInterval = pingInterval4NATType || RouteTableConfig.PingIntervalMS.dynamic(subBucket.distRank);
            if (now - lastTransTime.send.all >= pingInterval ||
                (shoudRetry(peer, subBucket.distRank) && now - lastTransTime.send.all >= RouteTableConfig.PingIntervalMS.Retry)) {

                if (!peer.isOnline(this.m_bucket.TIMEOUT_MS)) {
                    peer.address = null;
                }
                ping(peer);

                // 已经ping了，不需要再ping
                if (peer === lastRank.recentRecvPeer) {
                    lastRank.recentRecvPeer = null;
                }
            }
        });

        if (pingCount > 0) {
            this.m_lastPingTime = now;
        }
    }
}

module.exports = RouteTable;