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

const Base = require('../../base/base.js');
const {Result: DHTResult, Config} = require('../util.js');
const {Peer} = require('../peer.js');
const Task = require('./task.js');
const DHTPackage = require('../packages/package.js');
const DHTCommandType = DHTPackage.CommandType;
const {ResendControlor} = require('../package_sender.js');
const assert = require('assert');

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

const TaskConfig = Config.Task;
const HandshakeConfig = Config.Handshake;

function isEmptyEPList(eplist) {
    return !eplist || eplist.length == 0;
}

// isHoleImmediately = false表示先尝试自己向对方发起握手，几次失败后再试着通过中介peer进行打洞；否则两者同时进行，以最快速度建立连接
// passive = true表示优先通过中介让对方反向接入，一段时间后才主动向对方发起握手，可用于在实时性要求不高时检测本地网络是否能被新peer主动连接
class HandshakeSourceTask extends Task {
    constructor(owner, targetPeer, agencyPeer, isHoleImmediately, passive, handshakeSender) {
        super(owner, {timeout: HandshakeConfig.TimeoutMS, maxIdleTime: TaskConfig.MaxIdleTimeMS});

        this.m_id = Task.genGlobalTaskID(this.bucket.localPeer.peerid, this.m_id);
        this.m_handshakeResender = {sender: handshakeSender};
        if (handshakeSender) {
            this.m_handshakeResender.isDeposit = true;
        }
        this.m_targetPeer = targetPeer;
        this.m_holePackage = null;
        this.m_holeResender = null;
        if ( agencyPeer == null ) {
            this.m_agencyPeer = null
        } else {
            this.m_agencyPeer = this.bucket.findPeer(agencyPeer.peerid) || agencyPeer;
        }
        this.m_isHoleImmediately = isHoleImmediately;
        this.m_isPassive = passive;
        this.m_isIncoming = false;
    }

    get peerid() {
        return this.m_targetPeer.peerid;
    }

    onRemoteResponse(cmdPackage, remotePeer, remoteAddr, localAddr) {
        let connectedPeer = this._connectedPeer();
        this.m_targetPeer = connectedPeer || remotePeer;
        setImmediate(() => this._onComplete(DHTResult.SUCCESS));
    }

    _startImpl() {
        let connectedPeer = this._connectedPeer();
        if (connectedPeer) {
            this.m_targetPeer = connectedPeer;
            setImmediate(this._onComplete(DHTResult.SUCCESS));
            return;
        }

        let handshakePackage = null;
        if (!this.m_handshakeResender.sender) {
            handshakePackage = this.packageFactory.createPackage(DHTPackage.CommandType.HANDSHAKE_REQ);
            handshakePackage.body = {taskid: this.id};
            this.m_handshakeResender.sender = new ResendControlor(this.m_targetPeer,
                handshakePackage,
                this.packageSender,
                Peer.retryInterval(this.bucket.localPeer, this.m_targetPeer),
                Config.Package.RetryTimes - 1);
        }

        if ((!isEmptyEPList(this.m_targetPeer.eplist) || this.m_targetPeer.address) && !this.m_isPassive) {
            if (handshakePackage) {
                this.packageSender.sendPackage(this.m_targetPeer,
                    handshakePackage,
                    false,
                    Peer.retryInterval(this.bucket.localPeer, this.m_targetPeer));
            } else {
                this.m_handshakeResender.sender.send();
            }
        }
        
        // 本地有监听地址时才可能被反向穿透
        let localEPlist = this.bucket.localPeer.eplist;
        if (localEPlist && localEPlist.length > 0) {
            if (this.m_agencyPeer && (this.m_isHoleImmediately || this.m_isPassive || (isEmptyEPList(this.m_targetPeer.eplist) && !this.m_targetPeer.address))) {
                this.m_holePackage = this.packageFactory.createPackage(DHTPackage.CommandType.HOLE_CALL_REQ);
                this.m_holePackage.body = {
                    taskid: this.id,
                    target: {peerid: this.m_targetPeer.peerid, eplist: this.m_targetPeer.eplist},
                };
                this.m_holeResender = new ResendControlor(this.m_agencyPeer,
                    this.m_holePackage,
                    this.packageSender,
                    Peer.retryInterval(this.bucket.localPeer, this.m_agencyPeer),
                    Config.Package.RetryTimes);
                this.m_holeResender.send();
            }
        }
    }

    _processImpl(response, remotePeer) {
        LOG_DEBUG(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) remotePeer:${response.common.src.peerid} responsed HandshakeSourceTask(${this.m_targetPeer.peerid})`);
        let connectedPeer = this._connectedPeer();
        if (connectedPeer
            || response.cmdType === DHTPackage.CommandType.HANDSHAKE_REQ
            || response.cmdType === DHTPackage.CommandType.HANDSHAKE_RESP) {
            
            this.m_targetPeer = connectedPeer || remotePeer;
            this.m_isIncoming = (response.cmdType === DHTPackage.CommandType.HANDSHAKE_REQ);
            setImmediate(() => this._onComplete(DHTResult.SUCCESS));
            return;
        }

        if (response.cmdType === DHTPackage.CommandType.HOLE_CALL_RESP) {
            if (response.body.target && typeof response.body.target === 'object' &&
                typeof response.body.target.peerid === 'string' && response.body.target.peerid.length > 0) {
                if (response.body.target.peerid === this.m_targetPeer.peerid) {
                    let epCount = this.m_targetPeer.eplist? this.m_targetPeer.eplist.length : 0;
                    this.m_targetPeer.eplist = Peer.unionEplist(this.m_targetPeer.eplist, response.body.target.eplist);

                    let maxPassiveDelay = Math.max(Peer.retryInterval(this.bucket.localPeer, this.m_agencyPeer),
                        Peer.retryInterval(this.bucket.localPeer, this.m_targetPeer));
        
                    if ((!isEmptyEPList(this.m_targetPeer.eplist) || this.m_targetPeer.address) &&
                        (!this.m_isPassive || this.consum >= maxPassiveDelay)) {
            
                        this.m_handshakeResender.sender.send();
                    }
                } 
            }
            this.m_agencyPeer = null; // 打洞中介已经收到包，停止重发
        }
    }

    _retryImpl() {
        let connectedPeer = this._connectedPeer();
        if (connectedPeer) {
            this.m_targetPeer = connectedPeer;
            setImmediate(() => this._onComplete(DHTResult.SUCCESS));
            return;
        }
        let localEPlist = this.bucket.localPeer.eplist;
        if (localEPlist && localEPlist.length > 0) {
            if ((this.m_isHoleImmediately ||
                this.m_isPassive ||
                this.consum >= Peer.retryInterval(this.bucket.localPeer, this.m_targetPeer)) && this.m_agencyPeer) {

                if (!this.m_holePackage) {
                    this.m_holePackage = this.packageFactory.createPackage(DHTPackage.CommandType.HOLE_CALL_REQ);
                    this.m_holePackage.body = {
                        taskid: this.id,
                        target: {peerid: this.m_targetPeer.peerid, eplist: this.m_targetPeer.eplist},
                    };
                    this.m_holeResender = new ResendControlor(this.m_agencyPeer,
                        this.m_holePackage,
                        this.packageSender,
                        Peer.retryInterval(this.bucket.localPeer, this.m_agencyPeer),
                        Config.Package.RetryTimes);
                }
                this.m_holeResender.send();
            }
        }

        let maxPassiveDelay = Math.max(Peer.retryInterval(this.bucket.localPeer, this.m_agencyPeer),
            Peer.retryInterval(this.bucket.localPeer, this.m_targetPeer));

        if ((!isEmptyEPList(this.m_targetPeer.eplist) || this.m_targetPeer.address) &&
            (!this.m_isPassive || this.consum >= maxPassiveDelay)) {

            this.m_handshakeResender.sender.send();
        }
    }

    _onCompleteImpl(result) {
        LOG_DEBUG(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) HandshakeSourceTask(to:${this.m_targetPeer.peerid}) complete.`);
        this._callback(result, this.m_targetPeer, this.m_isIncoming);
        if (this.m_holeResender) {
            this.m_holeResender.finish();
        }
        // 外面托管的包归构造方管理
        if (this.m_handshakeResender && !this.m_handshakeResender.isDeposit && this.m_handshakeResender.sender) {
            this.m_handshakeResender.sender.finish();
        }
    }

    _connectedPeer() {
        let bucket = this.bucket;
        let peer = bucket.findPeer(this.m_targetPeer.peerid);
        if (peer && !peer.isTimeout(bucket.TIMEOUT_MS)) {
            return peer;
        }
        return null;
    }

    _stopImpl() {
    }
}

class HandshakeAgencyTask extends Task {
    constructor(owner, srcPeer, targetPeer, taskid) {
        super(owner, {timeout: HandshakeConfig.TimeoutMS, maxIdleTime: TaskConfig.MaxIdleTimeMS});

        this.m_id = taskid;
        this.m_holePackage = null;
        this.m_holeResender = null;
        this.m_srcPeer = srcPeer;
        this.m_targetPeer = targetPeer;
        this.m_isDone = false;
    }

    get srcPeerid() {
        return this.m_srcPeer.peerid;
    }

    get targetPeerid() {
        return this.m_targetPeer.peerid;
    }

    _startImpl() {
        this.m_holePackage = this.packageFactory.createPackage(DHTPackage.CommandType.HOLE_CALLED_REQ);
        this.m_holePackage.body = {
            taskid: this.id,
            src: {peerid: this.m_srcPeer.peerid, eplist: this.m_srcPeer.eplist},
        };
        this.m_holeResender = new ResendControlor(this.m_targetPeer,
            this.m_holePackage,
            this.packageSender,
            Peer.retryInterval(this.bucket.localPeer, this.m_targetPeer),
            Config.Package.RetryTimes);
        this.m_holeResender.send();
    }

    _processImpl(response, remotePeer) {
        LOG_DEBUG(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) remotePeer:${response.common.src.peerid} responsed HandshakeAgencyTask(${this.m_srcPeer.peerid}=>${this.m_targetPeer.peerid})`);

        if (response.cmdType === DHTPackage.CommandType.HOLE_CALLED_RESP) {
            // 收到响应包后只标记一下，协助打洞任务不需要任何结果，只等到超时就好了，后续有源peer发来的重发包直接忽略；
            // 如果这里立即完成，无法识别后续重发包
            this.m_isDone = true;
        }
    }

    _retryImpl() {
        if (this.m_isDone) {
            return;
        }

        this.m_holeResender.send();
    }

    _onCompleteImpl(result) {
        LOG_DEBUG(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) HandshakeAgencyTask(${this.m_srcPeer.peerid}=>${this.m_targetPeer.peerid}) complete.`);
        this.m_holeResender.finish();
    }

    _stopImpl() {
    }
}

class HandshakeTargetTask extends Task {
    constructor(owner, srcPeer, taskid) {
        // 这里timeout时间更长一点，避免被人恶意发送反连穿透请求对某特定节点进行DDOS攻击
        super(owner, {timeout: TaskConfig.TimeoutMS, maxIdleTime: TaskConfig.MaxIdleTimeMS});

        this.m_id = taskid;
        this.m_handshakeResender = null;
        this.m_srcPeer = srcPeer;
        this.m_isDone = false;
    }

    get peerid() {
        return this.m_srcPeer.peerid;
    }

    onRemoteResponse(cmdPackage, remotePeer, remoteAddr, localAddr) {
        this.m_isDone = true;
    }

    _startImpl() {
        // 打洞的目的是要src能连接上target，target是否能连接上src不能作为成功打洞的标准
        // if (this._isConnected()) {
        //     setImmediate(() => this._onComplete(DHTResult.SUCCESS));
        //     return;
        // }

        let handshakePackage = this.packageFactory.createPackage(DHTPackage.CommandType.HANDSHAKE_REQ);
        handshakePackage.body = {taskid: this.id};

        this.m_handshakeResender = new ResendControlor(this.m_srcPeer,
            handshakePackage,
            this.packageSender,
            Peer.retryInterval(this.bucket.localPeer, this.m_srcPeer),
            Config.Package.RetryTimes);
        this.m_handshakeResender.send();
    }

    _processImpl(response, remotePeer) {
        LOG_DEBUG(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) remotePeer:${response.common.src.peerid} responsed HandshakeTargetTask(${this.m_srcPeer.peerid})`);
        
        // 对方发来握手包，说明对方能连接上，可以结束了
        if (response.cmdType === DHTPackage.CommandType.HANDSHAKE_REQ
            || response.cmdType === DHTPackage.CommandType.HANDSHAKE_RESP) {
            
            // 跟Agency一样等超时，避免收到重发包重新握手
            // setImmediate(() => this._onComplete(DHTResult.SUCCESS));
            this.m_isDone = true;
            return;
        }
    }

    _retryImpl() {
        if (this.m_isDone) {
            return;
        }
        this.m_handshakeResender.send();
    }

    _onCompleteImpl(result) {
        LOG_DEBUG(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) HandshakeTargetTask(to:${this.m_srcPeer.peerid}) complete.`);
        this.m_handshakeResender.finish();
    }

    _stopImpl() {
    }
}

module.exports.Source = HandshakeSourceTask;
module.exports.Agency = HandshakeAgencyTask;
module.exports.Target = HandshakeTargetTask;
