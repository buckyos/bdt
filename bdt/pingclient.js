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

"use strict";
const packageModule = require('./package');
const BDT_ERROR = packageModule.BDT_ERROR;
const BDTPackage = packageModule.BDTPackage;
const EventEmitter = require('events');
const baseModule = require('../base/base');
const SNDHT = require('../sn/sn_dht');
const blog = baseModule.blog;
const BaseUtil = require('../base/util.js');
const assert = require('assert');
const SequenceU32 = BaseUtil.SequenceU32;
const TimeHelper = BaseUtil.TimeHelper;

class MultiSNPingClient extends EventEmitter {
    constructor(stack) {
        super();
        this.m_stack = stack;
        this.m_peerFinder = this.m_stack.peerFinder;
        this.m_state = PingClient.STATE.init;
        this.m_peerMap = {};
        this.m_snChangedListener = null;
        this.m_refreshSNTimer = null;
        this.m_lastSessionid = Math.floor(Math.random() * 100000);
        this.m_blackSNMap = {};
        this.m_lastSearchSNTime = 0;
    }

    get state() {
        return this.m_state;
    }

    get snList() {
        let snList = [];
        for (let [sessionid, pingClient] of Object.entries(this.m_peerMap)) {
            if (pingClient.state === PingClient.STATE.online) {
                snList.push(pingClient.sn);
            }
        }
        return snList;
    }

    get localEPList() {
        let epSet = new Set();
        for (let [sessionid, pingClient] of Object.entries(this.m_peerMap)) {
            pingClient.localEPList.forEach(ep => epSet.add(ep));
        }
        return [...epSet];
    }

    connect() {
        if (this.m_state === PingClient.STATE.init) {
            this.m_state = PingClient.STATE.connecting;
            let opt = this.m_stack._getOptions();

            let getRefreshInterval = () => {
                let total = 0;
                for (let [sessionid, pingClient] of Object.entries(this.m_peerMap)) {
                    total++;
                    if (pingClient.state === PingClient.STATE.online) {
                        return opt.searchSNInterval;
                    }
                }
                
                return total === 0? opt.searchSNIntervalInit : opt.searchSNIntervalConnecting;
            }

            const snLimit = opt.snLimit;
            let lastInterval = opt.searchSNIntervalInit;

            let tryRefreshSNList = () => {
                // 控制一下搜索频率
                let now = TimeHelper.uptimeMS();
                let diff = now - this.m_lastSearchSNTime;
                if (getRefreshInterval() !== opt.searchSNIntervalInit && diff < opt.searchSNIntervalInit) {
                    return;
                }
                this.m_lastSearchSNTime = now;
                this.m_peerFinder.findSN(this.m_stack.peerid).then(([error, peerlist]) => {
                        this.m_lastSearchSNTime = TimeHelper.uptimeMS();
                        if (error || !peerlist || !peerlist.length) {
                            return ;
                        }
                        if (peerlist.length > snLimit) {
                            peerlist = peerlist.slice(0, snLimit);
                        }

                        this._resetConnecting(peerlist);

                        let interval = getRefreshInterval();
                        if (lastInterval !== interval) {
                            clearInterval(this.m_refreshSNTimer);
                            this.m_refreshSNTimer = setInterval(tryRefreshSNList, interval);
                            lastInterval = interval;
                        }
                    });
            };

            this.m_snChangedListener = tryRefreshSNList;
            
            tryRefreshSNList();

            this.m_refreshSNTimer = setInterval(tryRefreshSNList, lastInterval);
            this.m_peerFinder.on(this.m_peerFinder.EVENT.SNChanged, tryRefreshSNList);
        } else {
            return BDT_ERROR.invalidState;
        }
    }

    close() {
        for (let peerid in this.m_peerMap) {
            this.m_peerMap[peerid].close();
            delete this.m_peerMap[peerid];
        }
        if (this.m_snChangedListener) {
            this.m_peerFinder.removeListener(this.m_peerFinder.EVENT.SNChanged, this.m_snChangedListener);
            this.m_snChangedListener = null;
        }
        if (this.m_refreshSNTimer) {
            clearInterval(this.m_refreshSNTimer);
            this.m_refreshSNTimer = null;
        }
        this.m_state = PingClient.STATE.closed;
    }

    _resetConnecting(peerlist) {
        if (!peerlist || !peerlist.length) {
            return;
        }

        let setBlack = peerid => {
            this.m_blackSNMap[peerid] = TimeHelper.uptimeMS();
        };
        let isBlack = peerid => {
            let t = this.m_blackSNMap[peerid];
            let now = TimeHelper.uptimeMS();
            if (t) {
                if (now - t < this.m_stack._getOptions().pingInterval) {
                    return true;
                }
                delete this.m_blackSNMap[peerid];
            }
            return false;
        }

        // 取第一个节点作为主节点，并以它的上下线作为上下线标志
        let peeridlist = new Set();
        let mainPeerid = null;
        peerlist.forEach(peer => {
            if (!isBlack(peer.peerid)) {
                peeridlist.add(peer.peerid);
                mainPeerid = mainPeerid || peer.peerid;
            }
        });
        if (peeridlist.size === 0) {
            return;
        }

        let now = TimeHelper.uptimeMS();
        for (let [sessionid, pingClient] of Object.entries(this.m_peerMap)) {
            if (peeridlist.has(pingClient.sn.peerid) && pingClient.state !== PingClient.STATE.offline) {
                pingClient.isMain = (pingClient.sn.peerid === mainPeerid);
                peeridlist.delete(pingClient.sn.peerid);
            } else {
                setBlack(pingClient.sn.peerid);
                pingClient.close();
                delete this.m_peerMap[sessionid];
            }
        }

        if (!this.m_connecting) {
            this.m_connecting = {};
        }
        this.m_connecting.startTime = now;
        
        for (let peer of peerlist) {
            this.m_stack._filterInvalidAddress(peer);
            if (peeridlist.has(peer.peerid)) {
                let sessionid = this._genSessionid();
                let pingClient = new PingClient(this.m_stack, peer, sessionid);
                pingClient.connect();
                pingClient.isMain = (peer.peerid === mainPeerid);
                // 主SN上线下线决定SN总体状态；任意SN下线就重新搜索SN
                pingClient.once(PingClient.EVENT.online, ()=>{
                    if (this.m_peerMap[sessionid] && pingClient.isMain) {
                        if (this.m_state !== PingClient.STATE.online) {
                            this.m_state = PingClient.STATE.online;
                            setImmediate(() => this.emit(PingClient.EVENT.online));
                        }
                    }
                });

                pingClient.once(PingClient.EVENT.offline, ()=>{
                    setBlack(pingClient.sn.peerid);
                    if (this.m_peerMap[sessionid]) {
                        if (pingClient.isMain) {
                            if (this.m_state !== PingClient.STATE.offline) {
                                this.m_state = PingClient.STATE.offline;
                                setImmediate(() => this.emit(PingClient.EVENT.offline));
                            }
                        }
                        if (this.m_snChangedListener) {
                            this.m_snChangedListener();
                        }
                    }
                });

                pingClient.on(PingClient.EVENT.nearSN, snPeerid => {
                    let newSNPingClient = this._getPingClientByPeerid(snPeerid);
                    if (newSNPingClient) {
                        if (newSNPingClient.state === PingClient.STATE.offline) {
                            newSNPingClient.close();
                            setBlack(snPeerid);
                            delete this.m_peerMap[newSNPingClient.sessionid];
                        } else {
                            return;
                        }
                    }

                    if (this.m_snChangedListener) {
                        this.m_snChangedListener();
                    }
                });

                pingClient.on(PingClient.EVENT.lostPackage, () => {
                    if (this.m_snChangedListener) {
                        this.m_snChangedListener();
                    }
                });

                this.m_peerMap[sessionid] = pingClient;
            }
        }
        peeridlist = null;
    }

    _onPackage(decoder, remoteSender) {
        let pingClient = this.m_peerMap[decoder.header.sessionid];
        if (pingClient) {
            pingClient._onPackage(decoder, remoteSender);
        }
    }

    _genSessionid() {
        let sessionid = this.m_lastSessionid;
        do {
            sessionid++;
            if (sessionid === 0x80000000) {
                sessionid = 1025;
            }
            assert(this.m_lastSessionid !== sessionid);
        } while (this.m_peerMap[sessionid]);
        this.m_lastSessionid = sessionid;
        return sessionid;
    }

    _getPingClientByPeerid(snPeerid) {
        for (let [sessionid, pingClient] of Object.entries(this.m_peerMap)) {
            if (pingClient.sn.peerid === snPeerid) {
                return pingClient;
            }
        }
        return null;
    }
}

class PingClient extends EventEmitter {
    constructor(stack, snPeer, sessionid) {
        super();
        this.m_stack = stack;
        this.m_state = PingClient.STATE.init;
        this.m_connecting = null;
        this.m_snSender = null;
        this.m_seq = stack.initSeq();
        this.m_ping = null;
        this.m_snPeer = {
            peerid: snPeer.peerid,
            peeridHash: BDTPackage.hashPeerid(snPeer.peerid),
            eplist: new Array(...snPeer.eplist)
        };

        this.m_initSender = this._initSender();
        this.m_isMain = false;
        this.m_pingInterval = stack._getOptions().pingInterval;
        this.m_sessionid = sessionid;
        this.m_localEPList = null;
    }

    get sn() {
        return this.m_snPeer;
    }

    get snSender() {
        return this.m_snSender;
    }

    get state() {
        return this.m_state;
    }

    get isMain() {
        return this.m_isMain;
    }

    set isMain(is) {
        this.m_isMain = is;
        this.m_pingInterval = this.m_stack._getOptions().pingInterval;
        // 不是主SN，把ping间隔拉长一倍
        if (!is) {
            this.m_pingInterval *= 2;
        }
    }

    get sessionid() {
        return this.m_sessionid;
    }

    get localEPList() {
        return this.m_localEPList || [];
    }

    connect() {
        if (this.m_state === PingClient.STATE.init) {
            this.m_state = PingClient.STATE.connecting;
            this.m_connecting = {
                startTime: TimeHelper.uptimeMS(),
                timer: null
            };

            this._tryConnect();
            return BDT_ERROR.success;
        } else {
            return BDT_ERROR.invalidState;
        }
    }

    close() {
        this._stopConnecting();
        this._stopPing();
        this.removeAllListeners(PingClient.EVENT.online);
        this.removeAllListeners(PingClient.EVENT.offline);
        this.removeAllListeners(PingClient.EVENT.nearSN);
        this.m_state = PingClient.STATE.closed;
    }

    _onPackage(decoder, remoteSender) {
        if (this.m_stack.remoteFilter.isForbidden(remoteSender.remoteEPList[0], this.m_snPeer.peerid)) {
            return;
        }
        
        let getProtocol = (sender) => {
            if (sender.remoteEPList.length > 0) {
                let addr = BaseUtil.EndPoint.toAddress(sender.remoteEPList[0]);
                return addr.protocol;
            }
        }

        let header = decoder.header;
        assert(header.sessionid === this.m_sessionid, `pkg.sessionid:${header.sessionid},sessionid:${this.m_sessionid}`);
        if (decoder.header.cmdType === BDTPackage.CMD_TYPE.pingResp) {
            if (Array.isArray(decoder.body.eplist)) {
                let epSet = new Set();
                decoder.body.eplist.forEach(ep => {
                    if (BaseUtil.EndPoint.toAddress(ep)) {
                        epSet.add(ep);
                    }
                });
                this.m_localEPList = [...epSet];
            }
            if (this.m_state < PingClient.STATE.connecting) {
                return ;
            }
            if (header.ackSeq === this.m_seq) {
                this.m_seq = SequenceU32.add(this.m_seq, 1);
            }
            let body = decoder.body;
            if (body.forward) {
                this._resetConnecting(body.forward);
            } else if (body.offline) {
                this._stopPing();
                this.m_state = PingClient.STATE.offline;
                setImmediate(() => this.emit(PingClient.EVENT.offline));
                return ;
            } else {
                let now = TimeHelper.uptimeMS();
                if (this.m_state === PingClient.STATE.connecting) {
                    this._stopConnecting();
                    this.m_state = PingClient.STATE.online;
                    this.m_snSender = remoteSender;
                    this.m_ping = {
                        timer: null,
                        lastRespTime: now,
                        lastUDPTime: getProtocol(remoteSender) === BaseUtil.EndPoint.PROTOCOL.tcp? 0 : now,
                    };

                    let opt = this.m_stack._getOptions();
                    const pingInterval = opt.pingInterval;
                    let lastPingTime = now;
                    this.m_ping.timer = setInterval(()=>{
                        let now = TimeHelper.uptimeMS();
                        let respInterval = now - this.m_ping.lastRespTime;
                        if (respInterval > opt.pingTimeout) {
                            this._stopPing();
                            this.m_state = PingClient.STATE.offline;
                            setImmediate(() => this.emit(PingClient.EVENT.offline));
                            return ;
                        } else if (respInterval > pingInterval + opt.pingLostTimeout) {
                            setImmediate(() => this.emit(PingClient.EVENT.lostPackage));
                        } else if (respInterval > pingInterval * 3) {
                            this.m_snSender.socket = null;
                        } else if (respInterval > pingInterval * 5) {
                            this.m_snSender.isResend = true;
                            // 太长时间没收到响应包，测试一下TCP，万一连通还能凑合一下
                            if (this.m_initSender.tcp) {
                                this.m_initSender.tcp.isResend = true;
                                this.m_initSender.tcp.postPackage(pingPkg, null, true, opt.pingDelay);
                            }
                        }

                        let pingPkg = this._createPingPackage();
                        if (this.m_isMain || now - lastPingTime >= this.m_pingInterval) {
                            if (this.m_snSender.postPackage(pingPkg, null, true, opt.pingDelay) !== this.m_stack.mixSocket.ERROR.success) {
                                this.m_snSender.isResend = true;
                                this.m_snSender.postPackage(pingPkg, null, true, opt.pingDelay);
                            }
                        }

                        // 太长时间没udp收发，一般是udp被禁的情况，但还是测试一下，万一突然打开了呢
                        if (getProtocol(this.m_snSender) === BaseUtil.EndPoint.PROTOCOL.tcp) {
                            if (now - this.m_ping.lastUDPTime > pingInterval * 5 &&
                                this.m_initSender.udp) {
                                    this.m_initSender.udp.isResend = true;
                                    this.m_initSender.udp.postPackage(pingPkg, null, true, opt.pingDelay);
                                    this.m_ping.lastUDPTime = now;
                            }
                        } else {
                            this.m_ping.lastUDPTime = now;
                        }
                    }, pingInterval);
                    setImmediate(() => this.emit(PingClient.EVENT.online));
                } else if (this.m_state === PingClient.STATE.online) {
                    if (getProtocol(remoteSender) === BaseUtil.EndPoint.PROTOCOL.udp) {
                        this.m_ping.lastUDPTime = now;
                        this.m_snSender = remoteSender;
                        this.m_snSender.isResend = false;
                    } else {
                        // 如果收到TCP响应包，等UDP完全没响应再把当前生效sender改成tcp
                        if (now - this.m_ping.lastUDPTime > this.m_stack._getOptions().pingInterval * 5) {
                            this.m_snSender = remoteSender;
                            this.m_snSender.isResend = false;
                        }
                    }
                    this.m_ping.lastRespTime = now;
                }

                if (body.nearSN) {
                    setImmediate(() => this.emit(PingClient.EVENT.nearSN, body.nearSN));
                }
            }
        }
    }

    _tryConnect() {
        // 尽量用udp，tcp协议无法穿透，纯TCP协议SN的价值在于所有节点都是公网节点，无需穿透
        let encoder = this._createPingPackage();
        const opt = this.m_stack._getOptions();
        if (this.m_initSender.udp) {
            this.m_initSender.udp.postPackage(encoder, null, true, opt.pingDelay);
        } else if (this.m_initSender.tcp) {
            this.m_initSender.tcp.postPackage(encoder, null, true, opt.pingDelay);
        } else {
            this.m_state = PingClient.STATE.offline;
            setImmediate(() => this.emit(PingClient.EVENT.offline));
            return;
        }

        let tryTimes = 0;
        let ping = () => {
            tryTimes++;
            if (tryTimes > 3) {
                if (this.m_initSender.tcp) {
                    this.m_initSender.tcp.postPackage(encoder, null, true, opt.pingDelay);
                    this.m_initSender.tcp.isResend = (tryTimes % 3 === 0);
                }
            }
            if (this.m_initSender.udp) {
                this.m_initSender.udp.postPackage(encoder, null, true, opt.pingDelay);
                this.m_initSender.udp.isResend = (tryTimes % 3 === 0);
            }
        }

        let pingInterval = opt.pingConnectInterval;
        let nextPingTime = 0;
        this.m_connecting.timer = setInterval(()=>{
            let now = TimeHelper.uptimeMS();
            if (now - this.m_connecting.startTime > opt.pingConnectTimeout) {
                this._stopConnecting();
                this.m_state = PingClient.STATE.offline;
                setImmediate(() => this.emit(PingClient.EVENT.offline));
                return ;
            }

            if (now > nextPingTime) {
                ping();
            }

            pingInterval *= 2;
            nextPingTime = now + pingInterval;
        }, opt.pingConnectInterval);

        ping();
    }

    _resetConnecting(remoteInfo) {
        this.m_snPeer = remoteInfo;
        this.m_snPeer.peeridHash = BDTPackage.hashPeerid(remoteInfo.peerid);
        
        if (this.m_state === PingClient.STATE.connecting) {
            this.m_connecting.startTime = TimeHelper.uptimeMS();
        } else if (this.m_state === PingClient.STATE.online) {
            this.m_state = PingClient.STATE.connecting;
            this._stopPing();
            this.m_connecting = {
                startTime: TimeHelper.uptimeMS(),
                timer: null
            };
            this._tryConnect();
        }
    }

    _stopConnecting() {
        if (this.m_connecting) {
            clearInterval(this.m_connecting.timer);
            this.m_connecting = null;
        }
    }


    _stopPing() {
        if (this.m_ping) {
            clearInterval(this.m_ping.timer);
            this.m_ping = null;
        }
    }


    _createPingPackage() {
        let listenEPList = null;
        if (this.m_stack._peerFinder()) {
            listenEPList = this.m_stack._peerFinder().getLocalEPList();
            if (listenEPList.length > 8) {
                if (this.m_isMain) {
                    listenEPList = listenEPList.slice(0, 8);
                } else {
                    listenEPList = listenEPList.slice(8, 16);
                }
            }
        }

        if (!listenEPList || listenEPList.length === 0) {
            listenEPList = this.m_stack.listenEPList;
        } else {
            listenEPList = [... new Set([...listenEPList, ...this.m_stack.listenEPList])];
        }

        let encoder = BDTPackage.createEncoder();
        let header = encoder.header;
        header.cmdType = BDTPackage.CMD_TYPE.pingReq;
        header.sessionid = this.m_sessionid;
        header.src = {
            peeridHash: this.m_stack.peeridHash 
        };
        header.dest = {
            peeridHash: this.m_snPeer.peeridHash
        };
        header.seq = this.m_seq;
        let body = encoder.body;
        body.peerid = this.m_stack.peerid;
        body.eplist = listenEPList;
        return encoder;
    }

    _initSender() {
        let initSender = {
            udp: null,
            tcp: null,
        };

        let classifyEPList = (eplist, protocol) => {
            let subEPList = [];
            eplist.forEach(ep => {
                let addr = BaseUtil.EndPoint.toAddress(ep);
                if (addr && addr.protocol === protocol) {
                    subEPList.push(ep);
                }
            });
            return subEPList;
        }

        let udpEPList = classifyEPList(this.m_snPeer.eplist, BaseUtil.EndPoint.PROTOCOL.udp);
        if (udpEPList.length > 0) {
            initSender.udp = BDTPackage.createSender(this.m_stack.mixSocket, null, udpEPList);
        }

        let tcpEPList = classifyEPList(this.m_snPeer.eplist, BaseUtil.EndPoint.PROTOCOL.tcp);
        if (tcpEPList.length > 0) {
            initSender.tcp = BDTPackage.createSender(this.m_stack.mixSocket, null, tcpEPList);
        }
        return initSender;
    }
}


PingClient.STATE = {
    init: 0,
    connecting: 1,
    online: 2,
    offline: 3,
    closed:  4,
};

PingClient.EVENT = {
    online: 'online',
    offline: 'offline',
    nearSN: 'nearSN',
    lostPackage: 'lostPackage',
};

module.exports.PingClient = PingClient;
module.exports.MultiSNPingClient = MultiSNPingClient;