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
const TimeHelper = BaseUtil.TimeHelper;

const LOG_TRACE = Base.BX_TRACE;
const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

let g_sendStat = null;

class PackageSender extends EventEmitter {
    constructor(mixSocket, bucket) {
        super();
        this.m_mixSocket = mixSocket;
        this.m_bucket = bucket;
        this.m_taskExecutor = null;
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
            peer.onlineDuration < Config.Peer.recommandNeighborTime &&
            !peer.__noRecommandNeighbor) {

            let closePeerList = this.m_bucket.findClosestPeers(peer.peerid);
            if (closePeerList && closePeerList.length > 0) {
                for (let closePeer of closePeerList) {
                    if (closePeer.isOnline(this.m_bucket.TIMEOUT_MS) &&
                        closePeer.peerid !== peer.peerid &&
                        closePeer.peerid !== localPeer.peerid) {
                        recommandNodes.push({id: closePeer.peerid, eplist: closePeer.eplist});
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

        LOG_DEBUG(`PEER(${this.m_bucket.localPeer.peerid}) Send package(${DHTCommandType.toString(cmdPackage.cmdType)}) to peer(${peer.peerid})`);

        let options = {
            ignoreCache: ignoreRouteCache,
            socket: null,
            onPreSend: (pkg, remoteAddr, socket, protocol) => this._onPreSendPackage(pkg, remoteAddr, socket, protocol, peer, recommandNodes),
            dropBusyTCP: true,
            timeout,
        };
        this.m_mixSocket.send(cmdPackage, eplist, options);
    }

    _onPreSendPackage(cmdPackage, remoteAddr, socket, protocol, peer, recommandNodes) {
        if (cmdPackage.__isTooLarge) {
            return null;
        }

        let now = TimeHelper.uptimeMS();
        let localPeer = this.m_bucket.localPeer;
        let peerStruct = localPeer.toStructForPackage();
        
        cmdPackage.fillCommon(peerStruct, peer, recommandNodes);
        if (!DHTCommandType.isResp(cmdPackage.cmdType)) {
            cmdPackage.common.packageID = g_sendStat.genPackageID();
        }
        
        cmdPackage.dest.ep = EndPoint.toString(remoteAddr);
        LOG_DEBUG(`PEER(${this.m_bucket.localPeer.peerid}) Send package(${DHTCommandType.toString(cmdPackage.cmdType)}) to peer(${cmdPackage.dest.peerid}|${peer.peerid}:${EndPoint.toString(remoteAddr)})`);
        
        let encoder = DHTPackageFactory.createEncoder(cmdPackage);
        let buffer = encoder.encode();

        if (buffer.length <= DHTPackageFactory.PACKAGE_LIMIT ||
            protocol === this.m_mixSocket.PROTOCOL.tcp ||
            !this.m_taskExecutor) {

            if (peer instanceof Peer.Peer &&
                !peer.__noRecommandNeighbor) {
                peer.__noRecommandNeighbor = true;
            }
            
            peer.lastSendTime = now;
            localPeer.lastSendTime = now;
            if (remoteAddr.protocol === EndPoint.PROTOCOL.udp) {
                peer.lastSendTimeUDP = now;
                localPeer.lastSendTimeUDP = now;
            }

            g_sendStat._onPkgSend(cmdPackage, buffer, peer, remoteAddr);
            return buffer;
        } else {
            // split package
            cmdPackage.__isTooLarge = true;
            this.m_taskExecutor.splitPackage(cmdPackage, peer);
            return null;
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

class SendStat {
    constructor(bucket) {
        assert(!g_sendStat);

        this.m_bucket = bucket;

        this.m_packageID = 0;
        this.m_packageTracer = []; // [{id, time}]
        
        // 记录向各peer发包情况，追踪可达peer的数据包丢失情况
        this.m_peerTracer = new Map(); // <peerid, [{time, length}]>
        this.m_traceClearTimer = null;

        this.m_stat = {
            udp: {
                pkgs: 0,
                bytes: 0,
            },
            tcp: {
                pkgs: 0,
                bytes: 0,
            },
            pkgs: new Map(),
        };
    }
    
    static create(bucket) {
        if (!g_sendStat) {
            g_sendStat = new SendStat(bucket);
        }
        return g_sendStat;
    }

    static stat() {
        return g_sendStat.stat();
    }

    stat() {
        return this.m_stat;
    }

    static onPackageGot(...args) {
        return g_sendStat.onPackageGot(...args);
    }

    onPackageGot(cmdPackage, remotePeer, remoteAddr, localAddr) {
        let now = TimeHelper.uptimeMS();

        if (DHTCommandType.isResp(cmdPackage.cmdType)) {
            // update RTT
            let packageID = cmdPackage.common.packageID;
            if (packageID) {
                let spliceCount = 0;
                for (let i = 0; i < this.m_packageTracer.length; i++) {
                    let tracer = this.m_packageTracer[i];
                    if (tracer.id === packageID) {
                        this.m_packageTracer.splice(0, i + 1);
                        let rtt = now - tracer.time;
                        remotePeer.updateRTT(rtt);
                        this.m_bucket.localPeer.updateRTT(rtt);
                        break;
                    }
                }
            }
        }

        {
            // 计数可达peer的发包数
            if (remoteAddr) {
                let sendTracer = this.m_peerTracer.get(remotePeer.peerid);
                if (sendTracer) {
                    for (let t of sendTracer) {
                        if (now - t.time < Config.Package.Timeout) {
                            let stat = this.m_stat.udp;
                            if (t.protocol === EndPoint.PROTOCOL.tcp) {
                                stat = this.m_stat.tcp;
                            }
                            stat.pkgs++;
                            stat.bytes += t.length;
                        }
                    }
                    this.m_peerTracer.delete(remotePeer.peerid);
                }
            }
        }
    }
    
    genPackageID() {
        this.m_packageID++;
        return this.m_packageID;
    }

    _onPkgSend(cmdPackage, buffer, remotePeer, remoteAddr) {
        if (!EndPoint.isNAT(remoteAddr)) {
            let now = TimeHelper.uptimeMS();
            let cmdType = cmdPackage.cmdType;
            if (cmdType === DHTCommandType.PACKAGE_PIECE_REQ) {
                cmdType = (DHTCommandType.PACKAGE_PIECE_REQ << 16) | cmdPackage.__orignalCmdType;
            }
            let count = this.m_stat.pkgs.get(cmdType) || 0;
            count++;
            this.m_stat.pkgs.set(cmdType, count);

            if (!DHTCommandType.isResp(cmdPackage.cmdType)) {
                this.m_packageTracer.push({id: cmdPackage.common.packageID, time: now});
            }

            // 记录发包时间
            let sendTracer = this.m_peerTracer.get(remotePeer.peerid);
            if (!sendTracer) {
                sendTracer = [{time: now, length: buffer.length, protocol: remoteAddr.protocol}];
                this.m_peerTracer.set(remotePeer.peerid, sendTracer);
            } else {
                sendTracer.push({time: now, length: buffer.length, protocol: remoteAddr.protocol});
            }

            // 定时清理
            if (!this.m_traceClearTimer) {
                this.m_traceClearTimer = setTimeout(() => {
                    this.m_traceClearTimer = null;
                    this._clearTimeoutTracer();
                }, Config.Package.Timeout * 2);
            }
        }
    }

    _clearTimeoutTracer() {
        // 清理超时记录
        let now = TimeHelper.uptimeMS();
        let timeoutPeerids = [];
        this.m_peerTracer.forEach((t, peerid) => {
            if (now - t[t.length - 1].time >= Config.Package.Timeout) {
                timeoutPeerids.push(peerid);
            } else if (now - t[0].time >= Config.Package.Timeout) {
                for (let i = 0; i < t.length; i++) {
                    if (now - t[i].time < Config.Package.Timeout) {
                        t.splice(0, i);
                        break;
                    }
                }
            }
        });
        timeoutPeerids.forEach(peerid => this.m_peerTracer.delete(peerid));
    }
}

module.exports.PackageSender = PackageSender;
module.exports.ResendControlor = ResendControlor;
module.exports.SendStat = SendStat;