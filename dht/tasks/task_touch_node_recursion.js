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
const Task = require('./task.js');
const {ResendControlor} = require('../package_sender.js');
const assert = require('assert');
const {Peer, LocalPeer} = require('../peer.js');
const BaseUtil = require('../../base/util.js');
const TimeHelper = BaseUtil.TimeHelper;

const TaskConfig = Config.Task;

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

// isForward: 控制该递归是否要向距离目标更近的范围缩小一步，
//          一般初始发起节点置false，取距离目标较近的节点，但不一定要比自己更近，以便在更大范围内搜索；
//          中间节点置true，只返回比自己更接近的节点
class TouchNodeTask extends Task {
    constructor(owner, {ttl = 0, isForward = false, timeout = TaskConfig.TimeoutMS, excludePeerids = null, isImmediately = true} = {}) {
        if (new.target === TouchNodeTask) {
            throw new Error('TouchNodeTask is a base frame, it must be extended.');
        }
        super(owner, {timeout});

        this.m_requestPeeridSet = new Set();
        this.m_pendingPeerList = new Map();

        this.m_ttl = ttl;
        this.m_isForward = isForward;
        this.m_isImmediately = isImmediately;
        this.m_package = null;
        this.m_excludePeerids = new Set([...(excludePeerids || [])]);
        this.m_arrivePeeridSet = new Set();
    }

    _startImpl() {
        this.m_package = this._createPackage();
        if (this.servicePath && this.servicePath.length > 0) {
            this.m_package.body.servicePath = this.servicePath;
        }

        this.m_package.common.ttl = this.m_ttl;
        
        let peerList = this._getInitTargetNodes();
        if (peerList.length === 0) {
            this._onComplete(DHTResult.FAILED);
            return;
        } else if (this._isExcludeLocalPeer) {
            let localPeer = this.bucket.localPeer;
            for (let i = 0; i < peerList.length; i++) {
                if (peerList[i].peerid === localPeer.peerid) {
                    peerList.splice(i, 1);
                    break;
                }
            }
        }

        for (let peer of peerList) {
            this._sendPackage(peer);
        }
    }

    _processImpl(response, remotePeer) {
        let arrivedPeerCount = this.m_arrivePeeridSet.size;

        if (response.body.r_nodes && Array.isArray(response.body.r_nodes)) {
            response.body.r_nodes.forEach(peerid => {
                if (typeof peerid !== 'string' || peerid.length === 0) {
                    return;
                }
                this.m_arrivePeeridSet.add(peerid);
                this.m_requestPeeridSet.add(peerid);
                let arrived = this.m_pendingPeerList.get(peerid);
                if (arrived) {
                    this.m_pendingPeerList.delete(peerid);
                    arrived.resender.finish();
                }
            });
        }

        this.m_arrivePeeridSet.add(response.src.peerid);
        let touchedPeerEx = this.m_pendingPeerList.get(response.common.src.peerid);
        if (touchedPeerEx) {
            this.m_pendingPeerList.delete(response.common.src.peerid);
            touchedPeerEx.resender.finish();
        }

        if (this.m_arrivePeeridSet.size !== arrivedPeerCount && this.m_package.body) {
            let eNodes = null;
            if (this.m_excludePeerids.size > 0) {
                eNodes = new Set([...this.m_arrivePeeridSet, ...this.m_excludePeerids]);
            } else {
                eNodes = this.m_arrivePeeridSet;
            }
            this.m_package.body.e_nodes = [...eNodes];
        }

        if (response.body.n_nodes && Array.isArray(response.body.n_nodes)) {
            let localPeer = this.bucket.localPeer;
            for (let peer of response.body.n_nodes) {
                if (peer && typeof peer.id === 'string' && peer.id.length > 0 &&
                    (!this._isExcludeLocalPeer || peer.id != localPeer.peerid)) {
                    this._sendPackage({peerid: peer.id, eplist: peer.eplist}, remotePeer);
                }
            }
        }

        if (this.m_pendingPeerList.size === 0) {
            this._onComplete(DHTResult.SUCCESS);
            return;
        }
    }

    _retryImpl() {
        let outtimePeers = [];
        for (let [peerid, peerEx] of this.m_pendingPeerList) {
            let self = this;
            if (!peerEx.resender.isTimeout()) {
                if (!peerEx.isHole) {
                    peerEx.resender.send();
                }
            } else {
                outtimePeers.push(peerid);
            }
        }

        outtimePeers.forEach(peerid => this.m_pendingPeerList.delete(peerid));

        if (this.m_pendingPeerList.size === 0) {
            if (!this.needRedo()) {
                this._onComplete(DHTResult.SUCCESS);
            } else {
                // 重做一遍
                this.m_requestPeeridSet.clear();
                this._startImpl();
            }
            return;
        }
    }

    _sendPackage(peer, agencyPeer) {
        if (this.m_ttl > 0) {
            // 对方收到包后需要转发处理，超时时间应该比本地任务短，否则可能本地任务超时结束还收不到对方的结果；
            // 1500ms作为对方超时判定和网络传输的冗余
            this.m_package.body.timeout = this.deadline - TimeHelper.uptimeMS() - 500;
            if (this.m_package.body.timeout <= 0) {
                delete this.m_package.body.timeout;
                this.m_ttl = 0;
                this.m_package.common.ttl = 0;
            }
        }

        if (!this.m_requestPeeridSet.has(peer.peerid)
            && !this.m_excludePeerids.has(peer.peerid)) {

            let peerEx = {
                peer,
                agencyPeer,
                isHole: false,
                resender: new ResendControlor(peer,
                    this.m_package,
                    this.packageSender,
                    Peer.retryInterval(this.bucket.localPeer, peer),
                    Config.Package.RetryTimes,
                    this.m_isImmediately),
            };
            this.m_pendingPeerList.set(peer.peerid, peerEx);
            this.m_requestPeeridSet.add(peer.peerid);
            if (agencyPeer) {
                // 如果有中转节点，走穿透逻辑
                peerEx.isHole = true;
                this.m_owner.handshakeSource(peerEx.peer, peerEx.agencyPeer, false, false, peerEx.resender, () => peerEx.isHole = false);
            } else {
                peerEx.resender.send();
            }
            return true;
        }
        return false;
    }

    get _isExcludeLocalPeer() {
        return true;
    }

    destroy() {
        this.m_pendingPeerList.forEach(peerEx => peerEx.resender.finish());
    }

    needRedo() {
        return false;
    }

    // override以下必须由子类重载
    _getInitTargetNodes() {
        throw new Error('TouchNodeTask._getInitTargetNodes it must be override.');
        // return nodeList;
    }

    _stopImpl() {
        throw new Error('TouchNodeTask._stopImpl it must be override.');
    }

    _createPackage() {
        throw new Error('TouchNodeTask._createPackage it must be override.');
        // return package;
    }

    _onCompleteImpl(result) {
        throw new Error('TouchNodeTask._onCompleteImpl it must be override.');
    }
}

class BroadcastNodeTask extends TouchNodeTask {
    constructor(owner, arrivePeerCount, {ttl = 0, isForward = false, timeout = TaskConfig.TimeoutMS, excludePeerids = null, isImmediately = true} = {}) {
        if (new.target === BroadcastNodeTask) {
            throw new Error('BroadcastNodeTask is a base frame, it must be extended.');
        }
        super(owner, {ttl, isForward, timeout, excludePeerids, isImmediately});
        this.m_arrivePeerCount = arrivePeerCount;
    }

    _getInitTargetNodes() {
        this.m_excludePeerids.add(this.bucket.localPeer.peerid);

        // <TODO> 测试代码
        this.bucket.forEachPeer(peer => {
            let serviceDescriptor = peer.findService(this.servicePath);
            assert(serviceDescriptor && serviceDescriptor.isSigninServer(), `peer:${JSON.stringify(peer.toStruct())},servicePath:${JSON.stringify(this.servicePath)}`);
        });

        const excludePeerids = new Set(...this.m_excludePeerids, ...this.m_arrivePeeridSet);
        return this.bucket.getRandomPeers({excludePeerids: excludePeerids, count: this.m_arrivePeerCount});
    }
}

class TouchNodeConvergenceTask extends TouchNodeTask {
    constructor(owner, {ttl = 0, isForward = false, timeout = TaskConfig.TimeoutMS, excludePeerids = null, isImmediately = true} = {}) {
        if (new.target === TouchNodeConvergenceTask) {
            throw new Error('TouchNodeConvergenceTask is a base frame, it must be extended.');
        }
        super(owner, {ttl, isForward, timeout, excludePeerids, isImmediately});
    }

    _getInitTargetNodes() {
        let option = this.m_isForward? {maxDistance: this.bucket.distanceToLocal(this._targetKey)} : {};
        if (this.m_excludePeerids.size > 0) {
            option.excludePeerids = new Set(...this.m_excludePeerids, ...this.m_arrivePeeridSet);
        }
        return this.bucket.findClosestPeers(this._targetKey, option);
    }

    // override 以下必须由子类重载
    get _targetKey() {
        throw new Error('TouchNodeConvergenceTask.key it must be override.');
        // return key;
    }
}

module.exports.TouchNodeTask = TouchNodeTask;
module.exports.TouchNodeConvergenceTask = TouchNodeConvergenceTask;
module.exports.BroadcastNodeTask = BroadcastNodeTask;