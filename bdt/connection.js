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
const EventEmitter = require('events');
const assert = require('assert');
const packageModule = require('./package');
const BDTStack = require('./stack');
const BDTTransfer = require('./transfer');
const TCPTransfer = require('./tcp_transfer');
const BDTPackage = packageModule.BDTPackage;
const BDT_ERROR = packageModule.BDT_ERROR;
const baseModule = require('../base/base');
const blog = baseModule.blog;
const BaseUtil = require('../base/util.js');
const EndPoint = BaseUtil.EndPoint;
const SequenceU32 = BaseUtil.SequenceU32;
const TimeHelper = BaseUtil.TimeHelper;
const {TCPConnectionMgr} = require('./tcp_connection_helper.js');

let _nextConnectionID = 1;

class BDTConnection extends EventEmitter {
    /*
        options:
            allowHalfOpen: true or false
            peerFinder: type of PeerFinder, use the finder in the stack to find the target-peer when null is set;
    */
    constructor(stack, options) {
        super();
        this.m_stack = stack;
        this.m_state = BDTConnection.STATE.init;
        this.m_createFrom = null;

        this.m_remote = {
            peerid: null,
            peeridHash: null,
            vport: null,
            sender: null,
            sessionid: null,
            isDynamic: false,
        };

        this.m_vport = null;
        this.m_sessionid = null;

        this.m_nextSeq = stack.initSeq();
        this.m_nextRemoteSeq = null;
        /*{
            timer: NodeTimer timer instance to clearInterval
            startTime:number start time
            vport:number vport to syn
            sender:[BDTPackageSender] sender to use
        }*/
        this.m_tryConnect = null;

        /*{
            timer: NodeTimer timer instance to clearInterval
        }*/
        this.m_snCall = null;

        this.m_peerFinderOp = null;
        this.m_dynamicSNCalled = null;

        if (this.m_stack.options.debug) {
//<<<<<<<<<统计诊断
            // DEBUG
            this.queryRemote = {
                ep: '', // 对方发包地址，如果跟从SN和DHT返回eplist不同，基本可以确定对方是对称NAT
                syn: 0, // 对方syn时间
                synConsum: 0, // 发出syn到收到synAck耗时
                sn: {
                    start: 0, // 开始查询SN的时间
                    finishsn: 0, // 查询到SN的时间
                    respEP1: 0, // SN第一次响应时间
                    respEP2: 0, // SN最后一次响应时间
                    getEP: 0, // 第一次返回对方地址的时间
                    snList: [],
                    eplist: [],
                    epTime: 3600000,
                },
                dht: {
                    start: 0,
                    finish: 0,
                    eplist: [],
                },
                flow: {
                    udp: {
                        send: {
                            pkgs: 0,
                            bytes: 0,
                        },
                        recv: {
                            pkgs: 0,
                            bytes: 0,
                        }
                    },
                    tcp: {
                        send: {
                            pkgs: 0,
                            bytes: 0,
                        },
                        recv: {
                            pkgs: 0,
                            bytes: 0,
                        }
                    }
                }
            };
//>>>>>>>>>>>>>>
        }


        this.m_respPackages = {};
        this.m_pendingDataPackages = [];

        this.m_transfer = null;

        let now = TimeHelper.uptimeMS();
        this.m_heartbeat = {
            lastRecvTime: now,
            lastSendTime: now
        };
        this.m_heartbeatTimer = null;

        this.m_useTCP = false;

        this.m_id = _nextConnectionID;
        _nextConnectionID++;

        this.m_options = {
            // 是否允许半开连接：
            // 如果置true，在收到'end'事件后，依旧可以向对端发送数据，直到不再需要connection后手动调用close关闭连接；
            // 默认值为false，在收到对端发来的'fin'包后自动回复一个'fin'关闭连接。
            allowHalfOpen: false,
        };

        if (options && options.allowHalfOpen) {
            this.m_options.allowHalfOpen = true;
        }

        if (options && options.peerFinder) {
            this._findSN = (peerid, fromCache, onStep) => options.peerFinder.findSN(peerid, fromCache, onStep);
            this._findPeer = peerid => options.peerFinder.findPeer(peerid);
        } else {
            this._findSN = (peerid, fromCache, onStep) => stack._findSN(peerid, fromCache, onStep);
            this._findPeer = peerid => stack._findPeer(peerid);
        }
    }

    get local() {
        return {
            peerid: this.m_stack.peerid,
            vport: this.m_vport,
            sessionid: this.m_sessionid,
        };
    }

    get remote() {
        return {
            peerid: this.m_remote.peerid,
            vport: this.m_remote.vport,
            sessionid: this.m_remote.sessionid,
            endpoint: this.m_remote.sender? this.m_remote.sender.activeEP : null,
        };
    }

    get useTCP() {
        return this.m_useTCP;
    }

    get id() {
        if (this.m_createFrom === BDTConnection.CREATE_FROM.acceptor) {
            return `<${this.m_remote.sessionid || 'u'}-${this.m_sessionid || `l(${this.m_id})`}>`;
        } else {
            return `<${this.m_sessionid || `l(${this.m_id})`}-${this.m_remote.sessionid || 'u'}>`;
        }
    }

    get state() {
        return this.m_state;
    }

    bind(vport) {
        if (this.m_stack.state !== BDTStack.STATE.created) {
            blog.error(`[BDT]: connection(id=${this.id}) bind when stack not create`);
            return BDT_ERROR.invalidState;
        }
        let err = BDT_ERROR.success;
        [err, vport] = this.m_stack._genVPort(vport, this);
        if (err) {
            blog.error(`[BDT]: connection(id=${this.id}) bind to vport failed for ${BDT_ERROR.toString(err)}`);
            return err;
        }

        let sessionid = null;
        [err, sessionid] = this.m_stack._genSessionid(this);
        if (err) {
            this.m_stack._releaseVPort(vport, this);
            blog.error(`[BDT]: connection(id=${this.id}) gen sessionid failed for ${BDT_ERROR.toString(err)}`);
            return err;
        }
        blog.info(`[BDT]: connection(id=${this.id}) bind to vport ${vport}`);
        this.m_createFrom = BDTConnection.CREATE_FROM.connect;
        this.m_vport = vport;
        this.m_sessionid = sessionid;
        delete this.m_id;
        return BDT_ERROR.success;
    }

    /*params:{
        peerid:string acceptor's peerid
        vport:number acceport's vport
    }
    */
    // event error
    // event connect
    connect(params, callback) {
        assert(this.m_vport);
        if (callback) {
            this._connect(params).then(()=>{
                callback();
            });
        } else {
            return this._connect(params);
        }
    }

    send(buffer) {
        if (this.m_state !== BDTConnection.STATE.establish &&
            this.m_state !== BDTConnection.STATE.closeWait) {
            return 0;
        }
        blog.debug(`[BDT]: connection(id=${this.id}) send, buffer.length=${buffer.length}`);
        return this.m_transfer.send(buffer);
    }

    close(force=false, callback=null) {
        this.m_userClosed = true;
        if (this.m_state === BDTConnection.STATE.closed) {
            if (callback) {
                callback();
            }
            return Promise.resolve();    
        }
        return new Promise((resolve)=>{
            blog.info(`[BDT]: connection(id=${this.id}) will be closed. and remote is ${this.m_remote.peerid}@${this.m_remote.vport}.`);
            this.once(BDTConnection.EVENT.close, ()=>{
                if (callback) {
                    callback();
                }
                resolve();
            });
            if (force) {
                this._changeState(BDTConnection.STATE.closed, force);
                return;
            }
            if (this.m_state < BDTConnection.STATE.establish) {
                this._changeState(BDTConnection.STATE.closed);
            } else if (this.m_state === BDTConnection.STATE.establish) {
                this._changeState(BDTConnection.STATE.finWait1);
            } else if (this.m_state === BDTConnection.STATE.closeWait) {
                this._changeState(BDTConnection.STATE.lastAck);
            } 
        }); 
    }

    get stack() {
        return this.m_stack;
    }

    _nextSeq(length) {
        let seq = this.m_nextSeq;
        this.m_nextSeq = SequenceU32.add(this.m_nextSeq, length);
        if (length !== 0) {
            blog.debug(`[BDT]: connection(id=${this.id}) update seq to ${this.m_nextSeq}`);
        }
        return seq; 
    }

    _setNextRemoteSeq(remoteSeq) {
        this.m_nextRemoteSeq = remoteSeq;
        blog.debug(`[BDT]: connection(id=${this.id}) update remote seq to ${remoteSeq}`);
        return this.m_nextRemoteSeq;
    }

    _getNextRemoteSeq() {
        return this.m_nextRemoteSeq;
    }
    
    _postPackage(encoder) {
        if (!this._onSendPackage) {
            this._onSendPackage = (packageBuffer, remoteAddr, socket, protocol) => {
                if (this.m_stack.options.debug) {
                    if (!EndPoint.isNAT(remoteAddr)) {
                        let stat = this.queryRemote.flow.udp;
                        if (protocol === EndPoint.PROTOCOL.tcp) {
                            stat = this.queryRemote.flow.tcp;
                        }
                        stat.send.pkgs++;
                        stat.send.bytes += packageBuffer.length;
                    }
                }
            }
        }

        // 与对方取得直接联系前，连接断开后，sender都是null
        if (this.m_remote.sender) {
            this.m_heartbeat.lastSendTime = TimeHelper.uptimeMS();
            this.m_remote.sender.postPackage(encoder, this._onSendPackage, !this.useTCP);
        }
    }

    _connect(params) {
        blog.info(`[BDT]: connection(id=${this.id}) begin connect to ${params.peerid}:${params.vport}`);
        let connectOp = new Promise(
            (resolve, reject)=>{
                this.once(BDTConnection.EVENT.error, (err)=>{
                    resolve(err);
                });
                this.once(BDTConnection.EVENT.connect, ()=>{
                    resolve(BDT_ERROR.success);
                });
                if (this.m_state !== BDTConnection.STATE.init) {
                    blog.warn(`[BDT]: connection(id=${this.id}) try to connect in state ${BDTConnection.STATE.toString(this.m_state)}`);
                    setImmediate(() => this.emit(BDTConnection.EVENT.error, BDT_ERROR.invalidState));
                    return ;
                }
                if (!this.m_vport) {
                    blog.warn(`[BDT]: connection(id=${this.id}) try to connect before bind to vport`);
                    setImmediate(() => this.emit(BDTConnection.EVENT.error, BDT_ERROR.conflict));
                    return ;
                }

                this.m_remote.peerid = params.peerid;
                this.m_remote.peeridHash = BDTPackage.hashPeerid(params.peerid);
                this.m_remote.vport = parseInt(params.vport);
                this._changeState(BDTConnection.STATE.waitAck,
                    BDTPackage.createSender(this.m_stack.mixSocket, null, [])
                );
            }
        );
        return connectOp;
    }

    _createFromAcceptor(params) {
        let { remote, acceptor }  = params;
        this.m_remote = {
            peerid: remote.peerid,
            peeridHash: remote.peeridHash,
            vport: remote.vport,
            sessionid: remote.sessionid,
        };
        this.m_acceptor = acceptor;
        this.m_vport = acceptor.vport;
        this.m_createFrom = BDTConnection.CREATE_FROM.acceptor;

        // generate local sessionid
        let [err, sessionid] = this.m_stack._genSessionid(this);
        if (err) {
            blog.error(`[BDT]: create connection(id=${this.id}) from acceptor on vport ${this.m_vport} with remote ${this.m_remote.peerid}:${this.m_remote.vport}:${this.m_remote.sessionid} failed, err = ${BDT_ERROR.toString(err)}`);
            return err;
        }
        this.m_sessionid = sessionid;
        blog.info(`[BDT]: create connection(id=${this.id}) from acceptor on vport ${this.m_vport} with remote ${this.m_remote.peerid}:${this.m_remote.vport}:${this.m_remote.sessionid}`);
        delete this.m_id;
        return BDT_ERROR.success;
    }

    _createPackageHeader(cmdType) {
        let encoder = BDTPackage.createEncoder();
        let header = encoder.header;
        header.useTCP = this.m_useTCP;
        header.cmdType = cmdType;
        header.dest = {
            peeridHash: this.m_remote.peeridHash,
            vport: this.m_remote.vport
        };
        header.src = {
            peeridHash: this.m_stack.peeridHash,
            vport: this.m_vport
        };
        header.sessionid = this.m_remote.sessionid;
        return encoder;
    }

    _createSynAckPackage(seq, ackSeq) {
        let encoder = this._createPackageHeader(BDTPackage.CMD_TYPE.synAck);
        encoder.header.seq = seq;
        encoder.header.ackSeq = ackSeq;
        encoder.header.sessionid = this.m_sessionid;
        encoder.body.src = this.m_stack.peerid;
        encoder.body.dest = this.m_remote.peerid;
        encoder.body.sessionid = this.m_remote.sessionid; // 对方sessionid

        return encoder;
    }

    _createSynPackage(seq) {
        let encoder = this._createPackageHeader(BDTPackage.CMD_TYPE.syn);
        encoder.header.seq = seq;
        encoder.header.sessionid = this.m_sessionid;
        encoder.body.src = this.m_stack.peerid;
        encoder.body.dest = this.m_remote.peerid;
        return encoder;
    }

    _createCallPackage(seq, isDynamic) {
        let encoder = this._createPackageHeader(BDTPackage.CMD_TYPE.callReq);
        encoder.header.seq = seq;
        encoder.header.sessionid = this.m_sessionid;
        encoder.body.src = this.m_stack.peerid;
        encoder.body.dest = this.m_remote.peerid;
        if (isDynamic) {
            encoder.isDynamic = isDynamic;
        }
        return encoder;
    }

    _createCalledRespPackage(called, isDynamic) {
        let encoder = this._createPackageHeader(BDTPackage.CMD_TYPE.calledResp);
        encoder.header.ackSeq = called.header.seq;
        encoder.header.sessionid = this.m_sessionid;
        encoder.body.src = this.m_stack.peerid;
        encoder.body.dest = this.m_remote.peerid;
        encoder.body.sessionid = this.m_remote.sessionid; // 对方sessionid
        if (isDynamic) {
            encoder.body.isDynamic = isDynamic;
        }
        
        return encoder;
    }

    _createSynAckAckPackage(seq) {
        let encoder = this._createPackageHeader(BDTPackage.CMD_TYPE.synAckAck);
        encoder.header.seq = seq;
        encoder.body.src = this.m_stack.peerid;
        encoder.body.dest = this.m_remote.peerid;

        return encoder;
    }

    _createHeartbeatPackage() {
        let encoder = this._createPackageHeader(BDTPackage.CMD_TYPE.heartbeat);
        return encoder;
    }

    _createHeartbeatRespPackage() {
        let encoder = this._createPackageHeader(BDTPackage.CMD_TYPE.heartbeatResp);
        return encoder;
    }

    _startHeartbeat() {
        if (this.m_heartbeatTimer) {
            return ;
        }

        let timeoutTimes = 0;
        let beginTimeout = this.m_stack._getOptions().heartbeatBeginTimeout;
        let breakTimeout = this.m_stack._getOptions().breakTimeout;
        let heartbeatInterval = this.m_stack._getOptions().heartbeatInterval;
        this.m_heartbeatTimer = setInterval(() => {
            let now = TimeHelper.uptimeMS();
            let timeout = now - this.m_heartbeat.lastRecvTime;
            if (timeout > beginTimeout) {
                timeoutTimes++;
                if (timeoutTimes % 3 === 0) {
                    if (this.m_remote.sender && !this.m_useTCP && !this.m_remote.isDynamic) {
                        this.m_remote.sender.socket = null;
                        this.m_remote.sender.isResend = true;
                    }
                }
                if (timeout > breakTimeout) {
                    this._changeState(BDTConnection.STATE.break, BDT_ERROR.remoteNoHeartbeat);
                } else {
                    if (now - this.m_heartbeat.lastSendTime > heartbeatInterval) {
                        this._postPackage(this._createHeartbeatPackage());
                    }
                }
            } else {
                timeoutTimes = 0;
            }
        }, heartbeatInterval);
    }

    _stopHeartbeat() {
        if (this.m_heartbeatTimer) {
            clearInterval(this.m_heartbeatTimer);
            this.m_heartbeatTimer = null;
        }
    }
 
    _refreshHeartbeat() {
        this.m_heartbeat.lastRecvTime = TimeHelper.uptimeMS();
    }

    _onPackage(decoder, remoteSender, isDynamic) {
        // 关闭或中断不再处理任何报文
        if (this.m_state === BDTConnection.STATE.closed || this.m_state === BDTConnection.STATE.break) {
            return;
        }

        let remoteEP = remoteSender.remoteEPList[0];

        if (decoder.header.cmdType === BDTPackage.CMD_TYPE.calledReq) {
            // 被禁用的地址
            if (this.m_stack.remoteFilter.isForbidden(remoteEP)) {
                return;
            }

            let calledResp = null;
            let state = this.m_state;
            if (this.m_state === BDTConnection.STATE.init) {
                let updateRemoteSender = BDTPackage.createSender(
                    this.m_stack.mixSocket,
                    null, 
                    decoder.body.eplist);
                blog.debug(`[BDT]: connection(id=${this.id}) update connecting remote address to ${decoder.body.eplist}`);
                calledResp = this._createCalledRespPackage(decoder, isDynamic);
                let calledResps = {};
                this.m_respPackages[calledResp.header.cmdType] = calledResps;
                calledResps[isDynamic] = calledResp;
                this._changeState(BDTConnection.STATE.waitAckAck, updateRemoteSender);

                if (decoder.body.dynamics && decoder.body.dynamics.length > 0) {
                    this._addRemoteEP(decoder.body.eplist, decoder.body.dynamics);
                }
            } else {
                if ((decoder.body.eplist && decoder.body.eplist.length > 0) ||
                    (decoder.body.dynamics && decoder.body.dynamics.length > 0)) {
                    this._addRemoteEP(decoder.body.eplist, decoder.body.dynamics);
                    blog.debug(`[BDT]: connection(id=${this.id}) update connecting remote address to ${decoder.body.eplist}`);
                }

                // sn重发的called 包在任何时候都要回复called resp
                let calledResps = this.m_respPackages[BDTPackage.CMD_TYPE.calledResp];
                if (!calledResps) {
                    calledResps = {};
                    this.m_respPackages[BDTPackage.CMD_TYPE.calledResp] = calledResps;
                }
                calledResp = calledResps[isDynamic];
                if (!calledResp) {
                    calledResp = this._createCalledRespPackage(decoder, isDynamic);
                    calledResps[isDynamic] = calledResp;
                }
            }

            remoteSender.postPackage(calledResp);
            
            // 动态地址
            if (state === BDTConnection.STATE.init ||
                state === BDTConnection.STATE.waitAckAck) {
                    this._tryDynamicCalled(decoder, remoteSender, isDynamic);
            }
            return ;
        } else if (decoder.header.cmdType === BDTPackage.CMD_TYPE.callResp) {
            // 被禁用的地址
            if (this.m_stack.remoteFilter.isForbidden(remoteEP)) {
                return;
            }

            let now = TimeHelper.uptimeMS();
            if (this.m_stack.options.debug) {
                if (this.queryRemote.sn.respEP1 === 0) {
                    this.queryRemote.sn.respEP1 = now;
                }
                if (decoder.body.eplist && decoder.body.eplist.length > 0) {
                    this.queryRemote.sn.getEP = this.queryRemote.sn.getEP || now;
                    this.queryRemote.sn.eplist = [... new Set([...this.queryRemote.sn.eplist, ...decoder.body.eplist])];
                    if (decoder.body.time !== -1) {
                        decoder.body.time = decoder.body.time || 0;
                        if (decoder.body.time > 24 * 3600000) {
                            decoder.body.time = Date.now() - decoder.body.time;
                        }
                        if (decoder.body.time >= 0 && decoder.body.time < this.queryRemote.sn.epTime) {
                            this.queryRemote.sn.epTime = decoder.body.time;
                        }
                    }
                }
                this.queryRemote.sn.respEP2 = now;
                // console.log(`snEP:${remoteEP},eplist:${decoder.body.eplist},time:${decoder.body.time},now:${now}`);
            }
            
            if (this.m_state === BDTConnection.STATE.waitAck) {
                if ((decoder.body.eplist && decoder.body.eplist.length > 0) ||
                    (decoder.body.dynamics && decoder.body.dynamics.length > 0)) {
                    this._addRemoteEP(decoder.body.eplist, decoder.body.dynamics);
                    blog.debug(`[BDT]: connection(id=${this.id}) update connecting remote address to ${decoder.body.eplist} & ${decoder.body.dynamics}`);
                }

                if (this.m_snCall) {
                    if (decoder.body.nearSN) {
                        this.m_snCall.onSNFound([decoder.body.nearSN]);
                    }
                    this.m_snCall.respTime = now;
                }
            }
        }

        // 被禁用的地址
        if (this.m_stack.remoteFilter.isForbidden(remoteEP, this.m_remote.peerid)) {
            return;
        }
        
        // TCP和动态socket应该始终确定remoteEP和socket
        if (this.m_remote.sender && !this.m_useTCP && !this.m_remote.isDynamic) {
            this.m_remote.sender.addRemoteEPList(remoteSender.remoteEPList);
            if (this.m_remote.sender.isResend) {
                this.m_remote.sender.isResend = false;
                this.m_remote.sender.socket = remoteSender.socket;
                this.m_remote.sender.updateActiveEP(remoteEP);
            }
        }

        let remoteAddr = EndPoint.toAddress(remoteEP);

        if (this.m_stack.options.debug) {
            if (this.m_state === BDTConnection.STATE.establish && !EndPoint.isNAT(remoteAddr)) {
                let stat = this.queryRemote.flow.udp;
                if (remoteAddr.protocol === EndPoint.PROTOCOL.tcp) {
                    stat = this.queryRemote.flow.tcp;
                }
                stat.recv.pkgs++;
                stat.recv.bytes += decoder.header.totalLength;
            }
        }

        this._refreshHeartbeat();
        if (decoder.header.cmdType === BDTPackage.CMD_TYPE.syn) {
            if (this.m_stack.options.debug) {
                this.queryRemote.ep = remoteEP;
            }
            if (this.m_createFrom === BDTConnection.CREATE_FROM.acceptor) {
                if (this.m_state === BDTConnection.STATE.init) {
                    blog.debug(`[BDT]: connection(id=${this.id}) update connecting remote address to ${remoteSender.remoteEPList}`);
                    this._changeState(BDTConnection.STATE.waitAckAck, remoteSender);
                } else if (!this.m_useTCP) {
                    // 任何时候收到syn 也应该回复ack， 防止ack丢失
                    // ack丢失可能是因为对方地址错误，更新一下
                    this._addRemoteEP(remoteSender.remoteEPList);
                    let ack = this.m_respPackages[BDTPackage.CMD_TYPE.synAck];
                    if (ack) {
                        // DEBUG
                        if (this.m_stack.options.debug) {
                            ack.body.reqTime = decoder.body.sendTime || 0;
                            ack.body.sendTime = TimeHelper.uptimeMS();
                        }

                        remoteSender.postPackage(ack);
                    }
                }
            }
        } else if (decoder.header.cmdType === BDTPackage.CMD_TYPE.synAck) {
            let now = TimeHelper.uptimeMS();
            if (this.m_stack.options.debug) {
                this.queryRemote.ep = this.queryRemote.ep || remoteEP;
                this.queryRemote.syn = this.queryRemote.syn || now;
                this.queryRemote.synConsum = this.queryRemote.synConsum || now - decoder.body.reqTime;
            }
            // console.log(`synAck: remoteEP:${remoteEP}`);
            if (this.m_state === BDTConnection.STATE.waitAck) {
                // 可确定是否用tcp
                this.m_useTCP = (remoteAddr.protocol === EndPoint.PROTOCOL.tcp);
                this.m_remote.sender = remoteSender;
                this.m_remote.isDynamic = isDynamic;
                this.m_remote.sender.updateActiveEP(remoteEP);
                this.m_remote.sessionid = decoder.header.sessionid;
                if (this.m_tryConnect && this.m_tryConnect.remoteSender && !this.m_useTCP && !isDynamic) {
                    this.m_remote.sender.addRemoteEPList(this.m_tryConnect.remoteSender.remoteEPList);
                }
                this._setNextRemoteSeq(decoder.nextSeq);
                let synAckAck = this._createSynAckAckPackage(this._nextSeq(1), decoder.header.seq);
                if (!this.m_useTCP) {
                    this.m_respPackages[synAckAck.header.cmdType] = synAckAck;
                }

                // DEBUG
                if (this.m_stack.options.debug) {
                    synAckAck.body.reqTime = decoder.body.sendTime || 0;
                    synAckAck.body.sendTime = now;
                }
                
                this.m_remote.sender.postPackage(synAckAck);
                this._changeState(BDTConnection.STATE.establish);
            } else if (!this.m_useTCP) {
                // 任何时候收到ack 也应该回复ackack， 防止ackack丢失
                let synAckAck = this.m_respPackages[BDTPackage.CMD_TYPE.synAckAck];
                assert(synAckAck);

                // DEBUG
                if (this.m_stack.options.debug) {
                    synAckAck.body.reqTime = decoder.body.sendTime || 0;
                    synAckAck.body.sendTime = now;
                }

                remoteSender.postPackage(synAckAck);
            }
        } else if (decoder.header.cmdType === BDTPackage.CMD_TYPE.synAckAck) {
            if (this.m_stack.options.debug) {
                this.queryRemote.ep = this.queryRemote.ep || remoteEP;
                this.queryRemote.syn = this.queryRemote.syn || TimeHelper.uptimeMS();
            }
            if (this.m_state === BDTConnection.STATE.waitAckAck) {
                // 可确定是否用tcp
                this.m_useTCP = decoder.header.useTCP;
                assert(!this.m_useTCP || remoteAddr.protocol === EndPoint.PROTOCOL.tcp);
                this.m_remote.sender = remoteSender;
                this.m_remote.isDynamic = isDynamic;
                this.m_remote.sender.updateActiveEP(remoteEP);
                if (this.m_tryConnect && this.m_tryConnect.remoteSender && !this.m_useTCP && !isDynamic) {
                    this.m_remote.sender.addRemoteEPList(this.m_tryConnect.remoteSender.remoteEPList);
                }

                this._setNextRemoteSeq(decoder.nextSeq);
                this._changeState(BDTConnection.STATE.establish);
                if (!this.m_useTCP) {
                    let prePkg = null;
                    this.m_pendingDataPackages.forEach(pkg => {
                        assert(!prePkg || SequenceU32.compare(pkg.header.seq, prePkg.header.seq) > 0);
                        prePkg = pkg;
                        this.m_transfer._onPackage(pkg);
                    });
                }
                this.m_pendingDataPackages = null;
            }
        } else if (decoder.header.cmdType === BDTPackage.CMD_TYPE.data
            ||decoder.header.cmdType === BDTPackage.CMD_TYPE.fin) {
            
            if (this.m_stack.options.debug) {
                this.queryRemote.ep = remoteEP;
            }
            assert(!this.m_useTCP || remoteAddr.protocol === EndPoint.PROTOCOL.tcp);
            if (this.m_transfer) {
                this.m_transfer._onPackage(decoder, remoteSender);
            } else if (this.m_state === BDTConnection.STATE.waitAckAck) {
                let pendingCount = this.m_pendingDataPackages.length;
                if (pendingCount === 0 || SequenceU32.compare(this.m_pendingDataPackages[pendingCount - 1].header.seq, decoder.header.seq) < 0) {
                    this.m_pendingDataPackages.push(decoder);
                } else {
                    let [pos, insertPos] = BaseUtil.algorithm.binarySearch(decoder,
                        this.m_pendingDataPackages,
                        (target, cursor) => SequenceU32.compare(target.header.seq, cursor.header.seq));
                    if (pos < 0) {
                        this.m_pendingDataPackages.splice(insertPos, 0, decoder);
                    }
                }
            }
        } else if (decoder.header.cmdType === BDTPackage.CMD_TYPE.heartbeat) {
            if (this.m_stack.options.debug) {
                this.queryRemote.ep = remoteEP;
            }
            remoteSender.postPackage(this._createHeartbeatRespPackage());
        } else if (decoder.header.cmdType === BDTPackage.CMD_TYPE.heartbeatResp) {
            if (this.m_stack.options.debug) {
                this.queryRemote.ep = remoteEP;
            }
            // do nothing
        }
    }
    
    _startSNCall(seq) {
        if (this.m_snCall) {
            return ;
        }
        
        if (this.m_stack.options.debug) {
            if (!this.queryRemote.sn.start) {
                this.queryRemote.sn.start = TimeHelper.uptimeMS();
            }
        }

        this.m_snCall = {
            recallTimer: null,
            snPeers: [],
            tryFindSNInterval: this.m_stack._getOptions().tryFindSNInterval,
            onSNFound: null,
            respTime: 0,
        };

        let resendTimes = 0;
        const tryCallInterval = this.m_stack._getOptions().tryConnectInterval;
        
        let postCallPackage = snPeers => {
            let now = TimeHelper.uptimeMS();

            let callPackage = this._createCallPackage(seq, false);
            callPackage.body.eplist = this.m_stack.eplist;
            let resendInterval = Math.max(3, this.m_snCall.snPeers.length);
            for (let peer of snPeers) {
                if (peer.nextCallTime > now) {
                    continue;
                }
                peer.nextCallTime = now + peer.callInterval;
                if (peer.callTimes > 2) {
                    peer.callInterval *= 2;
                }

                peer.sender.isResend = (resendTimes === peer.nextResendTime);
                peer.sender.postPackage(callPackage);
                peer.callTimes++;
                if (peer.sender.isResend) {
                    peer.sender.isResend = false;
                    peer.nextResendTime += resendInterval;
                }

                // 如果一次call没有成功，启动一个动态端口
                if (peer.callTimes >= 2 && !peer.dynamicSender) {
                    peer.dynamicSender = 'creating';
                    peer.dynamicRemoteSender = 'creating';
                    let dynamicSocket = this.m_stack._getDynamicSocket();
                    dynamicSocket.create().then(socket => {
                        if (socket) {
                            if (this.m_snCall) {
                                let dynamicCallPackage = this._createCallPackage(seq, true);
                                dynamicCallPackage.body.eplist = this.m_stack.eplist;

                                peer.dynamicSender = BDTPackage.createSender(dynamicSocket, socket, peer.sender.remoteEPList);
                                peer.dynamicRemoteSender = BDTPackage.createSender(dynamicSocket, socket, []);
                                peer.dynamicSender.postPackage(dynamicCallPackage);
                            } else {
                                dynamicSocket.releaseSocket(socket);
                            }
                        }
                    });
                }

                if (peer.dynamicSender && peer.dynamicSender !== 'creating') {
                    let dynamicCallPackage = this._createCallPackage(seq, true);
                    dynamicCallPackage.body.eplist = this.m_stack.eplist;
                    peer.dynamicSender.postPackage(dynamicCallPackage);
                }
            }
        }

        let startCall2SN = () => {
            if (!this.m_snCall) {
                return;
            }
            if (resendTimes === 3) {
                this._startPeerFinder();
            }
            // 试着通过DHT穿透一下
            if (resendTimes >= 2 && resendTimes <= this.m_snCall.snPeers.length) {
                let sn = this.m_snCall.snPeers[resendTimes - 1];
                if (!sn.isResponsed) {
                    this._findSN(sn.peerid);
                }
            }
            postCallPackage(this.m_snCall.snPeers);
            resendTimes++;
            this.m_snCall.recallTimer = setTimeout(startCall2SN, tryCallInterval);
        }

        let onSNFound = foundSNList => {
            if (!foundSNList || foundSNList.length === 0) {
                return;
            }
            
            if (this.m_stack.options.debug) {
                this.queryRemote.sn.finishsn = this.queryRemote.sn.finishsn || TimeHelper.uptimeMS();
                foundSNList.forEach(p => {
                    this.m_stack._filterInvalidAddress(p);
                    if (!p || p.eplist.length === 0) {
                        return;
                    }

                    for (let sn of this.queryRemote.sn.snList) {
                        if (sn.pid === p.peerid) {
                            return;
                        }
                    }
                    this.queryRemote.sn.snList.push({pid: p.peerid, eplist: p.eplist});
                });
            }

            // 追加到SN列表
            let nextResendTime = resendTimes + 3;
            let newSNList = [];
            foundSNList.forEach(peer => {
                this.m_stack._filterInvalidAddress(peer);
                if (!peer || peer.eplist.length === 0) {
                    return;
                }

                for (let existSN of this.m_snCall.snPeers) {
                    if (existSN.peerid === peer.peerid) {
                        return;
                    }
                }
                let sender = BDTPackage.createSender(this.m_stack.mixSocket, null, peer.eplist);
                sender.isResend = false;
                let snInfo = {
                    peerid: peer.peerid,
                    sender: sender,
                    dynamicSender: null,
                    dynamicRemoteSender: null,
                    nextCallTime: 0,
                    callInterval: tryCallInterval,
                    nextResendTime,
                    isResponsed: false,
                    callTimes: 0,
                };
                this.m_snCall.snPeers.push(snInfo);
                newSNList.push(snInfo);
                nextResendTime++;
            });

            if (newSNList.length > 0) {
                blog.debug(`[BDT]: connection(id=${this.id}) find new sn: ${JSON.stringify(newSNList.map(sn => sn.peerid))}`);
            }

            if (newSNList.length === this.m_snCall.snPeers.length) {
                // 第一次查到SN
                startCall2SN();
            } else if (newSNList.length > 0) {
                // 立即对新发现SN发起一次call
                postCallPackage(newSNList);
            }
        }

        this.m_snCall.onSNFound = onSNFound;

        let startSNFinder = (fromCache, once) => {
            let finder = this._findSN(this.m_remote.peerid, fromCache, ([err, peerlist]) => {
                if (this.m_snCall) {
                    onSNFound(peerlist);
                    return false;
                } else {
                    return true;
                }
            });

            finder.then(([err, peerlist]) => {
                // 准备下一轮查找SN
                if (this.m_snCall) {
                    if (!once) {
                        setTimeout(() => {
                            if (this.m_snCall) {
                                startSNFinder(false, false);
                                this._startPeerFinder();
                                this.m_snCall.tryFindSNInterval *= 2;
                            }
                        }, this.m_snCall.tryFindSNInterval);
                    }
                } else {
                    return;
                }

                if (!peerlist || peerlist.length === 0) {
                    this._startPeerFinder();
                    return ;
                }

                onSNFound(peerlist);
            });
        }

        startSNFinder(true, false);
        setTimeout(() => {
            // 防御snFinder返回时间太长，等它返回后出现异常就来不及了
            if (this.m_snCall) {
                if (this.m_snCall.snPeers.length === 0) {
                    startSNFinder(false, true);
                }
                this._startPeerFinder();
            }
        }, this.m_stack._getOptions().tryFindSNInterval * 2);
    }

    _onSNResponsed(peerid) {
        if (this.m_snCall) {
            for (let sn of this.m_snCall.snPeers) {
                if (sn.peerid === peerid) {
                    sn.isResponsed = true;
                    break;
                }
            }
        }
    }

    _stopSNCall() {
        if (this.m_snCall) {
            if (this.m_snCall.recallTimer) {
                clearTimeout(this.m_snCall.recallTimer);
            }
            
            for (let snInfo of this.m_snCall.snPeers) {
                if (snInfo.dynamicSender &&
                    snInfo.dynamicSender !== 'creating') {

                        snInfo.dynamicSender.abort();
                        if (this.m_remote.sender &&
                            snInfo.dynamicSender.socket !== this.m_remote.sender.socket) {
                                snInfo.dynamicSender.mixSocket.releaseSocket(snInfo.dynamicSender.socket);
                                snInfo.dynamicSender = null;
                        }
                    }
            }
            this.m_snCall = null;
        }
    }

    _startPeerFinder() {
        if (this.m_peerFinderOp) {
            return ;
        }

        if (this.m_stack.options.debug) {
            if (!this.queryRemote.dht.start) {
                this.queryRemote.dht.start = TimeHelper.uptimeMS();
            }
        }

        let finder = this._findPeer(this.m_remote.peerid);
        if (!finder) {
            return;
        }

        this.m_peerFinderOp = finder;
        
        let onFoundPeer = peer => {
            if (this.m_stack.options.debug) {
                if (!this.queryRemote.dht.finish1) {
                    this.queryRemote.dht.finish1 = TimeHelper.uptimeMS();
                }
            }

            if ((!peer || !peer.eplist || peer.eplist.length === 0)) {
                if (this.m_peerFinderOp) {
                    let tryFindPeerInterval = this.m_peerFinderOp.tryFindPeerInterval || this.m_stack._getOptions().tryFindSNInterval;
                    setTimeout(() => {
                        if (this.m_peerFinderOp) {
                            this._stopPeerFinder();
                            this._startPeerFinder();
                            this.m_peerFinderOp.tryFindPeerInterval = tryFindPeerInterval * 2;
                        }
                    }, tryFindPeerInterval);
                }
                return ;
            }

            if (this.m_stack.options.debug) {
                this.queryRemote.dht.finish = TimeHelper.uptimeMS();
                this.queryRemote.dht.eplist = peer.eplist;
            }

            if (this.m_peerFinderOp === finder) {
                if (this.m_state === BDTConnection.STATE.waitAck) {
                    if (peer.eplist.length) {
                        this._addRemoteEP(peer.eplist);
                        blog.debug(`[BDT]: connection(id=${this.id}) update connecting remote address to ${peer.eplist}`);
                    }
                }
            }
            this._stopPeerFinder();
        }

        let onNotSupport = () => {};
        finder.then(onFoundPeer, onNotSupport);
    }

    _stopPeerFinder() {
        this.m_peerFinderOp = null;
    }

    _startTryConnect(remoteSender, connectPackage) {
        assert(!this.m_tryConnect);
        if (this.m_tryConnect) {
            return ;
        }
        let opt = this.m_stack._getOptions();
        let tryTimes = 0;
        
        let tryConnectRoutine = ()=>{
            let now = TimeHelper.uptimeMS();
            let tryTime = now - this.m_tryConnect.startTime;
            if (tryTime > opt.connectTimeout) {
                const errorCode = () => {
                    const remoteEPList = this.m_tryConnect.remoteSender.remoteEPList;
                    if (remoteEPList && remoteEPList.length > 0) {
                        return BDT_ERROR.remoteNoSyn;
                    }

                    if (this.m_snCall) {
                        if (this.m_snCall.snPeers.length === 0) {
                            return BDT_ERROR.noSNFound;
                        }
                        if (!this.m_snCall.respTime) {
                            return BDT_ERROR.snNotResp;
                        } else {
                            return BDT_ERROR.noAddressFromSN;
                        }
                    }
                    return BDT_ERROR.timeout;
                }
                this._changeState(BDTConnection.STATE.break, errorCode());
                return;
            }

            let sender = this.m_tryConnect.remoteSender;
            
            if (opt.connectTimeout - tryTime <= opt.tryConnectInterval * 3 && opt.dynamicExpand > 0) {
                let guessEPList = [];
                this.m_tryConnect.dynamics.forEach(ep => {
                    let addr = BaseUtil.EndPoint.toAddress(ep);
                    if (!addr) {
                        return;
                    }
                    let port = addr.port;

                    let guess = (step, count) => {
                        addr.port = port;
                        for (let i = 0; i < count; i++) {
                            addr.port += step;
                            if (addr.port > 0 && addr.port <= 65535) {
                                guessEPList.push(BaseUtil.EndPoint.toString(addr));
                            }
                        }
                    }
                    
                    guess(-1, opt.dynamicExpand);
                    guess(1, opt.dynamicExpand);
                });
                sender.addRemoteEPList(guessEPList);
            }

            if (this.m_tryConnect.remoteSender.remoteEPList.length) {
                tryTimes++;
                // 每3次测试一下对方peer所有地址
                this.m_tryConnect.remoteSender.isResend = (tryTimes % 3 === 0);
                if (!this.m_tryConnect.remoteSender.socket && tryTimes <= 1) {
                    // 第一次只用udp测试一下，如果对方没响应就用全部地址测试
                    let udpEPList = [];
                    this.m_tryConnect.remoteSender.remoteEPList.forEach(ep => {
                        let address = EndPoint.toAddress(ep);
                        if (address && address.protocol === EndPoint.PROTOCOL.udp) {
                            udpEPList.push(ep);
                        }
                    });
                    if (udpEPList.length) {
                        sender = BDTPackage.createSender(this.m_stack.mixSocket, null, udpEPList);
                    }
                }

                // DEBUG
                if (this.m_stack.options.debug) {
                    connectPackage.body.reqTime = 0;
                    connectPackage.body.sendTime = now;
                }
                
                this.m_tryConnect.sendConnectPackage(sender);
            }

            if (this.m_snCall) {
                this.m_snCall.snPeers.forEach(snInfo => {
                    if (snInfo.dynamicRemoteSender && snInfo.dynamicRemoteSender !== 'creating') {
                        snInfo.dynamicRemoteSender.addRemoteEPList(sender.remoteEPList);
                        this.m_tryConnect.sendConnectPackage(snInfo.dynamicRemoteSender);
                    }
                });
            }
            
            if (this.m_dynamicSNCalled) {
                for (let [snEP, dynamicInfo] of Object.entries(this.m_dynamicSNCalled)) {
                    if (dynamicInfo.remoteSender && dynamicInfo.remoteSender !== 'creating') {
                        dynamicInfo.remoteSender.addRemoteEPList(sender.remoteEPList);
                        this.m_tryConnect.sendConnectPackage(dynamicInfo.remoteSender);
                    }
                }                    
            }
        };

        let sendConnectPackage = sender => {
            if (sender) {
                const now = TimeHelper.uptimeMS();
                if (sender.lastSendTime && !sender._tryConnect) {
                    sender._tryConnect = {
                        interval: opt.tryConnectInterval,
                        epcount: 0,
                    }
                }

                // 重试间隔翻倍
                if (sender._tryConnect) {
                    const epcount = sender.remoteEPList.length;
                    if (now - sender.lastSendTime < sender._tryConnect.interval &&
                        epcount === sender._tryConnect.epcount) {
                            return;
                    }
                    if (epcount !== sender._tryConnect.epcount) {
                        sender._tryConnect.interval = opt.tryConnectInterval;
                        sender._tryConnect.epcount = epcount;
                    } else {
                        sender._tryConnect.interval *= 2;
                    }
                }
                sender.postPackage(connectPackage);
            } else {
                tryConnectRoutine();
            }
        }

        this.m_tryConnect = {
            timer: setInterval(tryConnectRoutine, opt.tryConnectInterval),
            startTime: TimeHelper.uptimeMS(),
            remoteSender: remoteSender,
            sendConnectPackage,
            dynamics: new Set(),
        };
        
        tryConnectRoutine();

        if (this.m_stack.options.debug) {
            this.queryRemote.sn.eplist = [... new Set([...this.m_tryConnect.remoteSender.remoteEPList])];
        }
    }

    _stopTryConnect() {
        this._stopSNCall();
        this._stopPeerFinder();
        if (this.m_tryConnect) {
            clearInterval(this.m_tryConnect.timer);
            this.m_tryConnect = null;
        }

        if (this.m_dynamicSNCalled) {
            for (let [snEP, dynamicInfo] of Object.entries(this.m_dynamicSNCalled)) {
                if (dynamicInfo.sender &&
                    dynamicInfo.sender !== 'creating') {

                    dynamicInfo.sender.abort();

                    if (this.m_remote.sender &&
                        dynamicInfo.sender.socket !== this.m_remote.sender.socket) {
                        dynamicInfo.sender.mixSocket.releaseSocket(dynamicInfo.sender.socket);
                    }
                }
            }
            this.m_dynamicSNCalled = null;
        }
    }

    _addRemoteEP(eplist, dynamics) {
        if (this.m_tryConnect) {
            const filterAddress = eplist => {
                let peer = {
                    peerid: this.m_remote.peerid,
                    eplist,
                }
                return this.m_stack._filterInvalidAddress(peer).eplist;
            }
            eplist = eplist || [];
            eplist = filterAddress(eplist);
            if (dynamics) {
                dynamics = filterAddress(dynamics);
                dynamics = dynamics.filter(ep => {
                    if (!this.m_tryConnect.dynamics.has(ep)) {
                        this.m_tryConnect.dynamics.add(ep);
                        return true;
                    }
                });
                if (dynamics.length > 0) {
                    eplist = [...eplist, ...dynamics];
                }
            }

            let newEPList = this.m_tryConnect.remoteSender.addRemoteEPList(eplist);
            if (newEPList.length > 0) {
                let sender = null;
                if (newEPList.length < this.m_tryConnect.remoteSender.remoteEPList.length) {
                    sender = BDTPackage.createSender(this.m_stack.mixSocket, null, newEPList);
                }
                this.m_tryConnect.sendConnectPackage(sender);

                if (this.m_stack.options.debug) {
                    if (this.m_tryConnect) {
                        this.queryRemote.sn.eplist = [... new Set([...this.m_tryConnect.remoteSender.remoteEPList])];
                    }
                }
            }

            let add2Dynamic = dynamicSender => {
                let newEPList2 = dynamicSender.addRemoteEPList(this.m_tryConnect.remoteSender.remoteEPList);
                if (newEPList2.length > 0) {
                    let sender = dynamicSender;
                    if (newEPList2.length < dynamicSender.remoteEPList.length) {
                        sender = BDTPackage.createSender(dynamicSender.mixSocket, dynamicSender.socket, newEPList2);
                    }
                    this.m_tryConnect.sendConnectPackage(sender);
    
                    // DEBUG
                    if (this.m_stack.options.debug) {
                        this.queryRemote.sn.eplist = [... new Set([...dynamicSender.remoteEPList])];
                    }
                }
            }

            if (!this.m_tryConnect) {
                return;
            }

            if (this.m_dynamicSNCalled) {
                for (let [snEP, dynamicInfo] of Object.entries(this.m_dynamicSNCalled)) {
                    if (dynamicInfo.remoteSender && dynamicInfo.remoteSender !== 'creating') {
                        add2Dynamic(dynamicInfo.remoteSender);
                    }
                }
            }

            if (this.m_snCall) {
                this.m_snCall.snPeers.forEach(snInfo => {
                    if (snInfo.dynamicRemoteSender && snInfo.dynamicRemoteSender !== 'creating') {
                        add2Dynamic(snInfo.dynamicRemoteSender);
                    }
                });
            }
        }
    }

    _changeState(newState, params) {
        let curState = this.m_state;
        if (curState === newState) {
            return ;
        }
        
        blog.info(`[BDT]: connection(id=${this.id}) change state from ${BDTConnection.STATE.toString(this.m_state)} to ${BDTConnection.STATE.toString(newState)}, ${this.remote.endpoint}`);
        this.m_state = newState;
        
        if (newState === BDTConnection.STATE.establish) {
            assert(curState === BDTConnection.STATE.waitAck ||
                curState === BDTConnection.STATE.waitAckAck, `curState:${curState}`);

            this._stopTryConnect();
            this._startHeartbeat();
            let onLastAck = () => {
                if (this.m_state === BDTConnection.STATE.establish) {
                    this._changeState(BDTConnection.STATE.closeWait);
                    if (!this.m_options.allowHalfOpen) {
                        // 不允许半开连接就自动关闭
                        this.close();
                    }
                } else if (this.m_state === BDTConnection.STATE.finWait1) {
                    this._changeState(BDTConnection.STATE.closing);
                } else if (this.m_state === BDTConnection.STATE.finWait2) {
                    this._changeState(BDTConnection.STATE.timeWait); 
                }
            };
            if (this.m_useTCP) {
                this.m_transfer = new TCPTransfer(this, this.m_remote.sender, onLastAck);
                TCPConnectionMgr.register(this.m_remote.sender.socket, this);
            } else {
                this.m_transfer = new BDTTransfer(this, onLastAck);
            }
            if (this.m_createFrom === BDTConnection.CREATE_FROM.acceptor) {
                this.m_acceptor._onConnection(this);
            } else {
                setImmediate(()=>{this.emit(BDTConnection.EVENT.connect);});
            }
        } else if (newState === BDTConnection.STATE.finWait1
                || newState === BDTConnection.STATE.lastAck) {

            assert((newState === BDTConnection.STATE.finWait1 && curState === BDTConnection.STATE.establish) ||
                (newState === BDTConnection.STATE.lastAck && curState === BDTConnection.STATE.closeWait)
                , `curState:${curState},newState:${newState}`);

            this.m_transfer.sendFin(() => {
                if (this.m_state === BDTConnection.STATE.lastAck) {
                    this._changeState(BDTConnection.STATE.closed);
                } else if (this.m_state === BDTConnection.STATE.finWait1) {
                    this._changeState(BDTConnection.STATE.finWait2);
                } else if (this.m_state === BDTConnection.STATE.closing) {
                    this._changeState(BDTConnection.STATE.timeWait);
                }
            });
        } else if (newState === BDTConnection.STATE.timeWait) {
            assert(curState === BDTConnection.STATE.finWait2 ||
                curState === BDTConnection.STATE.closing, `curState:${curState}`);

            setTimeout(()=>{
                this._changeState(BDTConnection.STATE.closed);
            }, 2*this.m_stack._getOptions().msl);
        } else if (newState === BDTConnection.STATE.break) {
            assert(curState >= BDTConnection.STATE.waitAck, `curState:${curState}`);

            let errorCode = params;
            setImmediate(() => {
                if (!this.m_userClosed || this.listenerCount(BDTConnection.EVENT.error) > 0) {
                    this.emit(BDTConnection.EVENT.error, errorCode);
                }
            });
            this._changeState(BDTConnection.STATE.closed);
        } else if (newState === BDTConnection.STATE.closed) {
            const force = params;
            assert(force ||
                curState === BDTConnection.STATE.waitAck ||
                curState === BDTConnection.STATE.waitAckAck ||
                curState === BDTConnection.STATE.timeWait ||
                curState === BDTConnection.STATE.break ||
                curState === BDTConnection.STATE.lastAck
                , `curState:${curState}`);

            this.m_respPackages = {};
            this._stopTryConnect();
            this._stopHeartbeat();

            if (this.m_useTCP) {
                TCPConnectionMgr.unregister(this.m_remote.sender.socket, this);
            }

            if (this.m_transfer) {
                this.m_transfer.close();
                this.m_transfer = null;
            }
            if (this.m_createFrom === BDTConnection.CREATE_FROM.acceptor) {
                this.m_acceptor._unrefRemote(this.m_remote, this);
            } else if (this.m_createFrom === BDTConnection.CREATE_FROM.connect) {
                this.m_stack._releaseVPort(this.m_vport, this);
            }
            this.m_stack._releaseSessionid(this.m_sessionid, this);
            
            if (this.m_remote.sender) {
                this.m_remote.sender.abort();
                if (this.m_remote.isDynamic) {
                    this.m_remote.sender.mixSocket.releaseSocket(this.m_remote.sender.socket);
                    this.m_remote.sender = null;
                }
            }
            setImmediate(()=>{
                this.emit(BDTConnection.EVENT.close);
                this.removeAllListeners(BDTConnection.EVENT.connect);
                this.removeAllListeners(BDTConnection.EVENT.data);
                this.removeAllListeners(BDTConnection.EVENT.drain);
                this.removeAllListeners(BDTConnection.EVENT.error);
                this.removeAllListeners(BDTConnection.EVENT.end);
                this.removeAllListeners(BDTConnection.EVENT.close);
            });
        } else if (newState === BDTConnection.STATE.closeWait) {
            assert(curState === BDTConnection.STATE.establish, `curState:${curState}`);
        }

        if (curState === BDTConnection.STATE.init) {
            if (newState === BDTConnection.STATE.waitAck) {
                let remoteSender = params;
                let seq = this._nextSeq(1);
                this._startTryConnect(remoteSender, this._createSynPackage(seq));
                // 自连接直接定向到本地eplist
                if (this.m_remote.peerid === this.m_stack.peerid) {
                    this._addRemoteEP(this.m_stack.eplist);
                } else {
                    this._startSNCall(seq);
                }
            } else if (newState === BDTConnection.STATE.waitAckAck) {
                let remoteSender = params;
                let synAck = this._createSynAckPackage(this._nextSeq(1));
                this.m_respPackages[synAck.header.cmdType] = synAck;
                this._startTryConnect(remoteSender, synAck);
            } 
        }
    }

    _tryDynamicCalled(decoder, remoteSender, isDynamic) {
        let remoteEP = remoteSender.remoteEPList[0];
        if (!this.m_dynamicSNCalled) {
            this.m_dynamicSNCalled = {};
        }

        let dynamicInfo = this.m_dynamicSNCalled[remoteEP];
        if (!dynamicInfo) {
            dynamicInfo = {
                sender: null,
                remoteSender: null,
                responsed: false,
                times: 0,
            };
            this.m_dynamicSNCalled[remoteEP] = dynamicInfo;
        }

        dynamicInfo.times++;

        let createCalledResp = () => {
            // sn重发的called 包在任何时候都要回复called resp
            let calledResps = this.m_respPackages[BDTPackage.CMD_TYPE.calledResp];
            if (!calledResps) {
                calledResps = {};
                this.m_respPackages[calledResp.header.cmdType] = calledResps;
            }
            let calledResp = calledResps[true];
            if (!calledResp) {
                calledResp = this._createCalledRespPackage(decoder, true);
                calledResps[true] = calledResp;
            }
            return calledResp;
        }

        // 被多次called就启动动态地址
        if (dynamicInfo.times > 1 && !dynamicInfo.sender) {
            dynamicInfo.sender = 'creating';
            dynamicInfo.remoteSender = 'creating';

            let dynamicSocket = this.m_stack._getDynamicSocket();
            dynamicSocket.create().then(socket => {
                // 动态socket只是用于保底补救措施，失败了就算了，不再构造动态socket，再次构造很可能还是失败
                if (socket) {
                    if (this.m_dynamicSNCalled) {
                        dynamicInfo.sender = BDTPackage.createSender(dynamicSocket, socket, remoteSender.remoteEPList);
                        dynamicInfo.remoteSender = BDTPackage.createSender(dynamicSocket, socket, []);
                        dynamicInfo.sender.postPackage(createCalledResp());
                    } else {
                        dynamicSocket.releaseSocket(socket);
                    }
                }
            });
        }

        dynamicInfo.responsed = dynamicInfo.responsed || isDynamic;
        if (dynamicInfo.sender && 
            dynamicInfo.sender !== 'creating' &&
            !dynamicInfo.responsed) {

            dynamicInfo.sender.postPackage(createCalledResp());
        }
    }

    _onTCPDrain() {
        if (this.m_transfer) {
            this.m_transfer.trySendLeftData();
        }
    }

    _onTCPClose() {
        if (this.m_state === BDTConnection.STATE.establish) {
            this._changeState(BDTConnection.STATE.break, BDT_ERROR.connectionBreak);
        }
    }
}


BDTConnection.STATE = {
    init: -1,
    closed: 0,
    waitAck: 1,
    waitAckAck: 2,
    break: 3,
    establish: 4,
    finWait1: 5,
    finWait2: 6,
    closing: 7,
    timeWait: 8,
    closeWait: 9,
    lastAck: 10,

    toString(state) {
        if (state === BDTConnection.STATE.init) {
            return 'init';
        } else if (state === BDTConnection.STATE.closed) {
            return 'closed';
        } else if (state === BDTConnection.STATE.waitAck) {
            return 'waitAck';
        } else if (state === BDTConnection.STATE.waitAckAck) {
            return 'waitAckAck';
        } else if (state === BDTConnection.STATE.break) {
            return 'break';
        } else if (state === BDTConnection.STATE.establish) {
            return 'establish';
        } else if (state === BDTConnection.STATE.finWait1) {
            return 'finWait1';
        } else if (state === BDTConnection.STATE.finWait2) {
            return 'finWait2';
        } else if (state === BDTConnection.STATE.closing) {
            return 'closing';
        } else if (state === BDTConnection.STATE.timeWait) {
            return 'timeWait';
        } else if (state === BDTConnection.STATE.closeWait) {
            return 'closeWait';
        } else if (state === BDTConnection.STATE.lastAck) {
            return 'lastAck';
        }
    }
};

BDTConnection.CREATE_FROM = {
    connect: 0,
    acceptor: 1,
};

BDTConnection.EVENT = {
    error: 'error',
    connect: 'connect',
    close: 'close',
    data: 'data',
    drain: 'drain',
    end: 'end' // 收到对端发来的'fin'包
};

module.exports = BDTConnection;