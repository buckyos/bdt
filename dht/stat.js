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

const {EndPoint, Config} = require('./util.js');
const DHTPackage = require('./packages/package.js');
const DHTCommandType = DHTPackage.CommandType;
const BaseUtil = require('../base/util.js');
const TimeHelper = BaseUtil.TimeHelper;

let g_sendStat = null;

class Stat {
    constructor() {
        this.m_localPeers = [];

        this.m_packageID = 0;
        this.m_packageTracer = []; // [{id, time}]
        
        // 记录向各peer发包情况，追踪可达peer的数据包丢失情况
        this.m_peerTracer = new Map(); // <peerid, [{time, length}]>
        this.m_traceClearTimer = null;

        this.m_stat = {
            send: {
                udp: {
                    pkgs: 0,
                    bytes: 0,
                },
                tcp: {
                    pkgs: 0,
                    bytes: 0,
                },
                pkgs: new Map(),
            },
            recv: {
                udp: {
                    pkgs: 0,
                    bytes: 0,
                    req: 0,
                    resp: 0,
                },
                tcp: {
                    pkgs: 0,
                    bytes: 0,
                    req: 0,
                    resp: 0,
                },
                pkgs: new Map(),
            },
        };
    }
    
    static create() {
        return new Stat();
    }

    static addLocalPeer(anotherLocalPeer) {
        Stat._global().addLocalPeer(anotherLocalPeer);
    }

    addLocalPeer(anotherLocalPeer) {
        for (const peer of this.m_localPeers) {
            if (peer === anotherLocalPeer) {
                return;
            }
        }
        this.m_localPeers.push(anotherLocalPeer);
        if (this.m_localPeers.length > 0) {
            const existPeer = this.m_localPeers[0];
            anotherLocalPeer.RTT = existPeer.RTT;
            anotherLocalPeer.answer = existPeer.answer;
            anotherLocalPeer.question = existPeer.question;
        }
    }

    static removeLocalPeer(existLocalPeer) {
        Stat._global().removeLocalPeer(existLocalPeer);
    }
    
    removeLocalPeer(existLocalPeer) {
        for (let i = 0; i < this.m_localPeers.length; i++) {
            if (this.m_localPeers[i] === existLocalPeer) {
                this.m_localPeers.splice(i, 1);
                return;
            }
        }
    }

    static stat() {
        return Stat._global().stat();
    }

    stat() {
        return this.m_stat;
    }

    static onPackageRecved(...args) {
        return Stat._global().onPackageRecved(...args);
    }

    onPackageRecved(cmdPackage, remotePeer, remoteAddr, localAddr) {
        let now = TimeHelper.uptimeMS();

        // 更新RTT统计
        const updateRTT = () => {
            if (DHTCommandType.isResp(cmdPackage.cmdType) && cmdPackage.src.peerid !== cmdPackage.dest.peerid) {
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

                            this.m_localPeers.forEach(localPeer => localPeer.updateRTT(rtt));
                            break;
                        }
                    }
                }
            }
        }

        // 延迟统计发送包，因为部分包发向的目标地址不可达，所以在收到对方发包后才计数
        const sendCoundDelay = () => {
            // 计数可达peer的发包数
            if (remoteAddr && cmdPackage.src.peerid !== cmdPackage.dest.peerid) {
                let sendTracer = this.m_peerTracer.get(remotePeer.peerid);
                if (sendTracer) {
                    for (let t of sendTracer) {
                        if (now - t.time < Config.Package.Timeout) {
                            let stat = this.m_stat.send.udp;
                            if (t.protocol === EndPoint.PROTOCOL.tcp) {
                                stat = this.m_stat.send.tcp;
                            }
                            stat.pkgs++;
                            stat.bytes += t.length;
                        }
                    }
                    this.m_peerTracer.delete(remotePeer.peerid);
                }
            }
        }

        updateRTT();
        sendCoundDelay();
    }

    static statRecvFlowrate(...args) {
        Stat._global().statRecvFlowrate(...args);
    }

    statRecvFlowrate(cmdPackage, remoteAddr, messageLength) {
        if (!EndPoint.isNAT(remoteAddr)) {
            let stat = this.m_stat.recv.tcp;
            if (remoteAddr.protocol === EndPoint.PROTOCOL.udp) {
                stat = this.m_stat.recv.udp;
            }
            stat.pkgs++;
            stat.bytes += messageLength;
    
            let isResp = DHTCommandType.isResp(cmdPackage.cmdType);
            if (isResp) {
                stat.resp++;
            } else {
                stat.req++;
            }

            this.m_localPeers.forEach(localPeer => localPeer.onPackageRecved(isResp));

            let cmdType = cmdPackage.cmdType;
            if (cmdPackage.__isCombine) {
                cmdType = (DHTCommandType.PACKAGE_PIECE_REQ << 16) | cmdType;
            }
            let count = this.m_stat.recv.pkgs.get(cmdType) || 0;
            count++;
            this.m_stat.recv.pkgs.set(cmdType, count);
        }
    }

    static genPackageID() {
        return Stat._global().genPackageID();
    }

    genPackageID() {
        this.m_packageID++;
        return this.m_packageID;
    }

    static onPkgSend(cmdPackage, buffer, remotePeer, remoteAddr) {
        return Stat._global().onPkgSend(cmdPackage, buffer, remotePeer, remoteAddr);
    }

    onPkgSend(cmdPackage, buffer, remotePeer, remoteAddr) {
        if (!EndPoint.isNAT(remoteAddr)) {
            let now = TimeHelper.uptimeMS();
            let cmdType = cmdPackage.cmdType;
            if (cmdType === DHTCommandType.PACKAGE_PIECE_REQ) {
                cmdType = (DHTCommandType.PACKAGE_PIECE_REQ << 16) | cmdPackage.__orignalCmdType;
            }
            let count = this.m_stat.send.pkgs.get(cmdType) || 0;
            count++;
            this.m_stat.send.pkgs.set(cmdType, count);

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

    static _global() {
        if (!g_sendStat) {
            g_sendStat = new Stat();
        }
        return g_sendStat;
    }
}

module.exports = Stat;