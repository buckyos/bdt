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

const EventEmitter = require('events');
const Base = require('../base/base.js');
const {HashDistance, EndPoint, Config} = require('./util.js');
const Bucket = require('./bucket.js');
const DHTPackageFactory = require('./package_factory.js');
const DHTPackage = require('./packages/package.js');
const Peer = require('./peer.js');
const DHTCommandType = DHTPackage.CommandType;
const assert = require('assert');
const BaseUtil = require('../base/util.js');
const Stat = require('./stat.js');
const TimeHelper = BaseUtil.TimeHelper;

const LOG_TRACE = Base.BX_TRACE;
const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

class PackageSender extends EventEmitter {
    constructor(mixSocket, bucket) {
        super();
        this.m_mixSocket = mixSocket;
        this.m_bucket = bucket;
        this.m_taskExecutor = null;
        this.m_pendingPkgs = new Set(); // <'peerid@seq'>
        this.m_pendingQueue = []; // {'peerid@seq', time}
    }
    
    get mixSocket() {
        return this.m_mixSocket;
    }

    set taskExecutor(newValue) {
        this.m_taskExecutor = newValue;
    }

    sendPackage(toPeer, cmdPackage, ignoreRouteCache, timeout) {
        let localPeer = this.m_bucket.localPeer;
        // toPeer可能不是在本地路由表中记录的PEER对象；
        // 在发送时需要更新路由表中PEER对象的一些统计信息，
        // 所以这里要从路由表中重新查找一下
        let peer = this.m_bucket.findPeer(toPeer.peerid) || toPeer;
        if (!peer.hash) {
            peer.hash = HashDistance.hash(peer.peerid);
        }

        if (peer.peerid === localPeer.peerid) {
            let peerStruct = localPeer.toStructForPackage();
            cmdPackage.fillCommon(peerStruct, peer, []);
            setImmediate(() => this.emit(PackageSender.Events.localPackage, cmdPackage));
            return;
        }

        let recommandNodes = [];

        if (peer instanceof Peer.Peer &&
            !peer.__noRecommandNeighbor) {

            let recommandPeerList = null;
            const onlineDurationMS = peer.onlineDuration * 1000;
            if (onlineDurationMS < (Config.Peer.recommandNeighborTime >>> 2)) {
                recommandPeerList = this.m_bucket.findClosestPeers(peer.peerid);
            } else if (onlineDurationMS < Config.Peer.recommandNeighborTime) {
                recommandPeerList = this.m_bucket.getRandomPeers();
            }
            if (recommandPeerList && recommandPeerList.length > 0) {
                for (let recommandPeer of recommandPeerList) {
                    if (recommandPeer.isOnline(this.m_bucket.TIMEOUT_MS) &&
                        recommandPeer.peerid !== peer.peerid &&
                        recommandPeer.peerid !== localPeer.peerid) {
                        recommandNodes.push({id: recommandPeer.peerid, eplist: recommandPeer.eplist});
                    }
                }
            }
        }
        
        cmdPackage.__isTooLarge = false;

        // 合并peer和toPeer两个对象的eplist+address
        let eplist = peer.eplist || [];
        let addr = peer.address;
        if (addr) {
            eplist = Peer.Peer.unionEplist(eplist, [EndPoint.toString(addr)]);
        }
        if (toPeer !== peer) {
            if (toPeer.eplist) {
                eplist = Peer.Peer.unionEplist(eplist, toPeer.eplist);
            }
            addr = toPeer.address;
            if (addr) {
                eplist = Peer.Peer.unionEplist(eplist, [EndPoint.toString(addr)]);
            }
        }

        if (!eplist || eplist.length === 0) {
            return;
        }

        LOG_DEBUG(`[DHT(${cmdPackage.appid})] PEER(${this.m_bucket.localPeer.peerid}) Send package(${DHTCommandType.toString(cmdPackage.cmdType)}) to peer(${peer.peerid})`);

        const pendingKey = this._onPreparePkg(toPeer, cmdPackage);

        let options = {
            ignoreCache: ignoreRouteCache,
            socket: null,
            onPreSend: (pkg, remoteAddr, socket, protocol) => this._onPreSendPackage(pkg, remoteAddr, socket, protocol, peer, recommandNodes, {pendingKey, ignoreCache: ignoreRouteCache}),
            dropBusyTCP: true,
            timeout,
        };
        this.m_mixSocket.send(cmdPackage, eplist, options);
    }

    onPackageRecved(cmdPackage, remotePeer, remoteAddr, localAddr) {
        if (DHTCommandType.isResp(cmdPackage.cmdType)) {
            const pendingKey = `${remotePeer.peerid}@${cmdPackage.ackSeq}`;
            this.m_pendingPkgs.delete(pendingKey);
            // 队列中的内容等溢出阈值被触发时顺序清除
        }
    }

    _onPreSendPackage(cmdPackage, remoteAddr, socket, protocol, peer, recommandNodes, options) {
        if (cmdPackage.__isTooLarge) {
            return null;
        }

        if (!DHTCommandType.isResp(cmdPackage.cmdType)) {
            // 已经被响应的包不再send
            // 但是可能因为调用方忽略历史通信记录cache，希望强制对某些地址发包，就不阻止它，对所有地址都发送一个包
            if (!options.ignoreCache && !this._isPackagePending(options.pendingKey)) {
                return null;
            }
            cmdPackage.common.packageID = Stat.genPackageID();
        }

        let now = TimeHelper.uptimeMS();
        let localPeer = this.m_bucket.localPeer;
        let peerStruct = localPeer.toStructForPackage();
        
        cmdPackage.fillCommon(peerStruct, peer, recommandNodes);
        
        cmdPackage.dest.ep = EndPoint.toString(remoteAddr);
        LOG_DEBUG(`[DHT(${cmdPackage.appid})] PEER(${this.m_bucket.localPeer.peerid}) Send package(${DHTCommandType.toString(cmdPackage.cmdType)}) to peer(${cmdPackage.dest.peerid}|${peer.peerid}:${EndPoint.toString(remoteAddr)})`);
        
        let encoder = DHTPackageFactory.createEncoder(cmdPackage);
        let buffer = encoder.encode();

        if (buffer.length <= DHTPackageFactory.PACKAGE_LIMIT ||
            protocol === this.m_mixSocket.PROTOCOL.tcp ||
            !this.m_taskExecutor) {

            if (peer instanceof Peer.Peer &&
                !peer.__noRecommandNeighbor) {
                peer.__noRecommandNeighbor = true;
            }
            
            if (peer !== localPeer) {
                peer.lastSendTime = now;
                localPeer.lastSendTime = now;
                if (remoteAddr.protocol === EndPoint.PROTOCOL.udp) {
                    peer.lastSendTimeUDP = now;
                    localPeer.lastSendTimeUDP = now;
                }
            }

            Stat.onPkgSend(cmdPackage, buffer, peer, remoteAddr);
            return buffer;
        } else {
            // split package
            cmdPackage.__isTooLarge = true;
            setImmediate(() => this.m_taskExecutor.splitPackage(cmdPackage, peer));
            return null;
        }
    }

    _onPreparePkg(toPeer, cmdPackage) {
        if (!DHTCommandType.isResp(cmdPackage.cmdType)) {
            let pendingKey = `${toPeer.peerid}@${cmdPackage.seq}`;
            if (this.m_pendingPkgs.has(pendingKey)) {
                cmdPackage.updateSeq();
                pendingKey = `${toPeer.peerid}@${cmdPackage.seq}`;
            }

            const now = TimeHelper.uptimeMS();
            this.m_pendingPkgs.add(pendingKey);

            const info = {
                pendingKey,
                time: now,
                timeout: Peer.Peer.retryInterval(this.m_bucket.localPeer, toPeer),
            };
            this.m_pendingQueue.push(info);

            function isTimeout(info) {
                return now - info.time > info.timeout;
            }

            // 集中清理超时pending
            if (this.m_pendingQueue.length > 89 && isTimeout(this.m_pendingQueue[89])) {
                let timeoutCount = 0;
                for (timeoutCount = 0; timeoutCount < this.m_pendingQueue.length; timeoutCount++) {
                    const info = this.m_pendingQueue[timeoutCount];
                    if (isTimeout(info)) {
                        this.m_pendingPkgs.delete(info.pendingKey);
                    } else {
                        break;
                    }
                }
                this.m_pendingQueue.splice(0, timeoutCount);
            }
            return pendingKey;
        }
    }

    _isPackagePending(pendingKey) {
        if (pendingKey) {
            return this.m_pendingPkgs.has(pendingKey);
        } else {
            return true; // 默认是pending状态，需要再次发送
        }
    }
}

PackageSender.Events = {
    localPackage: 'localPackage',
}

let RESENDER_ID = 1;
let g_resenderMap = new Map();
function removeTimeoutResender() {
    let now = TimeHelper.uptimeMS();
    if (g_resenderMap.size > 809) {
        // 先把超时包去掉
        let timeoutResenders = [];
        g_resenderMap.forEach((resender, id) => {
            if (resender.isTimeout() || 
                resender.isFinish() || 
                now - resender.lastSendTime > 600809) {

                resender.abort();
                timeoutResenders.push(id);
            }
        });

        if (g_resenderMap.size > 809) {
            g_resenderMap.forEach((resender, id) => {
                if (resender.tryTimes > 2) {
                    resender.abort();
                    timeoutResenders.push(id);
                }
            });
        }

        timeoutResenders.forEach(id => g_resenderMap.delete(id));
    }
}

class ResendControlor {
    // 如果不设置peer/pkg/sender，不能调用send，自己调用needResend判定是否需要resend，调用sender.sendPackage后调用onSend控制下次重试的节奏
    // 如果设置了peer/pkg/sender，可以随时调用send重试一次，send函数内部决定是否真的到了重试的时机
    // 内部不设定时器自动resend，使用方需要resend时需手动触发，不用担心任务完成还有额外的resend包发出
    constructor(peer = null, pkg = null, sender = null, initInterval = 1000, timesLimit = 5, isImmediately = true) {
        this.m_peer = peer;
        this.m_pkg = pkg;
        this.m_sender = sender;

        this.m_interval = initInterval;
        this.m_tryTimes = 0;
        this.m_timesLimit = timesLimit;
        this.m_timesLimitForce = timesLimit;
        this.m_lastSendTime = 0;
        this.m_isImmediately = isImmediately;
        this.m_isFinish = false;
        this.m_id = RESENDER_ID++;

        g_resenderMap.set(this.m_id, this);
        removeTimeoutResender();
    }

    send() {
        if (!(this.m_peer && this.m_pkg && this.m_sender && this.needResend())) {
            return;
        }

        this.onSend();
        let delay = (this.m_isImmediately && this.m_tryTimes === 1)? 0 : (this.m_interval >> 1);
        this.m_sender.sendPackage(this.m_peer, this.m_pkg, (this.m_tryTimes % 2 === 0), delay);
        if (this.isTimeout()) {
            g_resenderMap.delete(this.m_id);
        }
    }

    onSend() {
        this.m_lastSendTime = TimeHelper.uptimeMS();
        this.m_tryTimes++;
        if (this.m_tryTimes >= 2) {
            this.m_interval *= 2;
        }
    }

    needResend() {
        return !this.isTimeout() && TimeHelper.uptimeMS() >= this.lastSendTime + this.m_interval;
    }

    isTimeout() {
        return this.m_tryTimes >= Math.min(this.m_timesLimit, this.m_timesLimitForce);
    }

    abort() {
        this.m_timesLimitForce = 0;
        g_resenderMap.delete(this.m_id);
    }

    finish() {
        this.m_isFinish = true;
        g_resenderMap.delete(this.m_id);
    }

    isFinish() {
        return this.m_isFinish;
    }

    get tryTimes() {
        return this.m_tryTimes;
    }
    
    get lastSendTime() {
        return this.m_lastSendTime;
    }

    get peer() {
        return this.m_peer;
    }
}

module.exports.PackageSender = PackageSender;
module.exports.ResendControlor = ResendControlor;
