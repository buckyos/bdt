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

const Base = require('../base/base.js');
const BaseUtil = require('../base/util.js');
const {Config, GetValueFlag, TOTAL_KEY} = require('./util.js');
const {Peer} = require('./peer.js');
const Bucket = require('./bucket.js');
const DHTPackage = require('./packages/package.js');
const DHTCommandType = DHTPackage.CommandType;
const DestributedValueTable = require('./distributed_value_table.js');
const HandshakeTask = require('./tasks/task_handshake.js');
const assert = require('assert');

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

const ValueTableConfig = Config.ValueTable;
const GetValueConfig = Config.GetValue;
const BucketConfig = Config.Bucket;

class PackageProcessor {
    constructor({bucket, taskMgr, taskExecutor, packageSender, packageFactory, distributedValueTable, broadcastEventEmitter}) {
        this.m_bucket = bucket;
        this.m_taskMgr = taskMgr;
        this.m_taskExecutor = taskExecutor;
        this.m_packageSender = packageSender;
        this.m_packageFactory = packageFactory;
        this.m_distributedValueTable = distributedValueTable;
        this.m_broadcastEventEmitter = broadcastEventEmitter;
        this.m_servicePath = [];
    }

    process(cmdPackage, remotePeer, remoteAddr, localAddr) {
        this.m_taskExecutor.onPackageRecved(cmdPackage, remotePeer, remoteAddr, localAddr);
        switch (cmdPackage.cmdType) {
            case DHTCommandType.FIND_PEER_REQ:
                this._processFindPeer(cmdPackage, remotePeer);
                break;
            case DHTCommandType.UPDATE_VALUE_REQ:
                this._processUpdateValue(cmdPackage, remotePeer);
                break;
            case DHTCommandType.FIND_VALUE_REQ:
                this._processFindValue(cmdPackage, remotePeer);
                break;
            case DHTCommandType.PING_REQ:
                let pingRespPackage = this.m_packageFactory.createPackage(DHTPackage.CommandType.PING_RESP);
                pingRespPackage.common.packageID = cmdPackage.common.packageID;
                pingRespPackage.common.ackSeq = cmdPackage.common.seq;
                this.m_packageSender.sendPackage(remotePeer, pingRespPackage);
                break;
            case DHTCommandType.PING_RESP: // fallthrough
                // nothing
                break;
            case DHTCommandType.HOLE_CALL_REQ:
                this._processHoleCall(cmdPackage, remotePeer);
                break;
            case DHTCommandType.HOLE_CALLED_REQ:
                this._processHoleCalled(cmdPackage, remotePeer);
                break;
            case DHTCommandType.HANDSHAKE_REQ:
                this._processHandshake(cmdPackage, remotePeer);
                let task = this.m_taskMgr.getTaskByID(cmdPackage.body.taskid);
                if (task) {
                    task.process(cmdPackage, remotePeer);
                }
                break;
            case DHTCommandType.BROADCAST_EVENT_REQ:
                this._processBroadCastEvent(cmdPackage, remotePeer);
                break;
            case DHTCommandType.PACKAGE_PIECE_REQ:
                LOG_ASSERT(false, `should not reach here. DHTCommandType.PACKAGE_PIECE_REQ`);
                break;
            case DHTCommandType.COMBINE_PACKAGE:
                // <TODO> 支持向同一个peer发送的多个包打包发送，节省带宽
                break;
            // 以下响应包的后续流程都封装在task里，由task接口统一处理响应包
            case DHTCommandType.HOLE_CALL_RESP:
            case DHTCommandType.HOLE_CALLED_RESP:
            case DHTCommandType.HANDSHAKE_RESP:
            case DHTCommandType.FIND_PEER_RESP: // fallthrough
            case DHTCommandType.FIND_VALUE_RESP:
            case DHTCommandType.UPDATE_VALUE_RESP:
            case DHTCommandType.BROADCAST_EVENT_RESP: 
            case DHTCommandType.PACKAGE_PIECE_RESP: {
                if (cmdPackage.body && cmdPackage.body.taskid) {
                    let task = this.m_taskMgr.getTaskByID(cmdPackage.body.taskid);
                    if (task) {
                        task.process(cmdPackage, remotePeer);
                    }
                }
                break;
            }
            default:
                LOG_ASSERT(false, `Unknown package received:${DHTCommandType.toString(cmdPackage.cmdType)}`);
        }
    }

    set servicePath(newValue) {
        this.m_servicePath = newValue;
    }

    // findPeer不支持TTL，因为找到的目标节点要保证可达，而经过跳转找到的peer对本地来讲，未必可达
    _processFindPeer(cmdPackage, remotePeer) {
        LOG_DEBUG(`LOCALPEER:(${this.m_bucket.localPeer.peerid}:${this.m_servicePath}) got findpeer command(${cmdPackage.body.target}) from peer(${cmdPackage.common.src.peerid})`);
        if (cmdPackage.body.target && typeof cmdPackage.body.target !== 'string') {
            return;
        }

        let respPackage = this.m_packageFactory.createPackage(DHTCommandType.FIND_PEER_RESP);
        respPackage.common.packageID = cmdPackage.common.packageID;
        this._fillRecursionRespPackageBody(respPackage, cmdPackage, cmdPackage.body.target);
        this.m_packageSender.sendPackage(remotePeer, respPackage);
    }

    _processUpdateValue(cmdPackage, remotePeer) {
        LOG_DEBUG(`LOCALPEER:(${this.m_bucket.localPeer.peerid}:${this.m_servicePath}) got updatevalue command(${cmdPackage.body.tableName}:${cmdPackage.body.key}) from peer(${cmdPackage.common.src.peerid})`);

        let {tableName, values} = cmdPackage.body;
        if (typeof tableName === 'string' && tableName.length > 0) {
            values.forEach(keyValue => this.m_distributedValueTable.updateValue(tableName, keyValue[0], keyValue[1]));
        }

        let response = (arrivedPeerids) => {
            let respPackage = this.m_packageFactory.createPackage(DHTCommandType.UPDATE_VALUE_RESP);
            respPackage.common.packageID = cmdPackage.common.packageID;
            this._fillRecursionRespPackageBody(respPackage, cmdPackage, tableName, arrivedPeerids, cmdPackage.body.e_nodes, false);
            this.m_packageSender.sendPackage(remotePeer, respPackage);
        }

        if (cmdPackage.common.ttl > 0) {
            this.m_taskExecutor.updateValue(tableName,
                new Map(values),
                {ttl: cmdPackage.common.ttl - 1, isForward: true, timeout: cmdPackage.body.timeout, excludePeerids: cmdPackage.body.e_nodes},
                (result, peerids) => response(peerids));
        } else {
            response();
        }
    }

    _processFindValue(cmdPackage, remotePeer) {
        let values = null;
        if (!('flags' in cmdPackage.body)) {
            cmdPackage.body.flags = GetValueFlag.Precise;
        }

        let {tableName, key, flags} = cmdPackage.body;
        LOG_DEBUG(`LOCALPEER:(${this.m_bucket.localPeer.peerid}:${this.m_servicePath}) got findvalue command(${tableName}:${key}:${flags}) from peer(${cmdPackage.common.src.peerid})`);

        if (typeof tableName !== 'string' || tableName.length === 0 ||
            typeof key !== 'string' || key.length === 0) {
            return;
        }
            
        if (key === TOTAL_KEY
            || (flags === GetValueFlag.Precise)) {
            values = this.m_distributedValueTable.findValue(tableName, key);
        } else if (flags === GetValueConfig.KeyHashClose) {
            values = this.m_distributedValueTable.findClosestValues(tableName, key);
        } else if (flags === GetValueConfig.UpdateLatest) {
            values = this.m_distributedValueTable.findLatestValues(tableName, key);
        }
        
        let response = (arrivedPeerids) => {
            let respPackage = this.m_packageFactory.createPackage(DHTCommandType.FIND_VALUE_RESP);
            respPackage.common.packageID = cmdPackage.common.packageID;
            respPackage.body = {};
            if (values) {
                respPackage.body.values = [...values];
            }
    
            this._fillRecursionRespPackageBody(respPackage, cmdPackage, tableName, arrivedPeerids, cmdPackage.body.e_nodes, !!values);
            this.m_packageSender.sendPackage(remotePeer, respPackage);
        }

        if (!values && cmdPackage.common.ttl > 0) {
            this.m_taskExecutor.getValue(tableName,
                key,
                flags,
                {ttl: cmdPackage.common.ttl - 1, isForward: true, timeout: cmdPackage.body.timeout, excludePeerids: cmdPackage.body.e_nodes},
                (result, foundValues, arrivedPeerids) => {
                    values = foundValues;
                    response(arrivedPeerids);
                });
        } else {
            response();
        }
    }

    _processBroadCastEvent(cmdPackage, remotePeer) {
        let {event, params, source} = cmdPackage.body;
        LOG_DEBUG(`LOCALPEER:(${this.m_bucket.localPeer.peerid}:${this.m_servicePath}) got broadcast event(${event}:${params}) from peer(${source})`);

        if (typeof event !== 'string' || event.length === 0 ||
            !source || typeof source.peerid !== 'string' || source.peerid.length === 0) {
            return;
        }

        let timeout = cmdPackage.body.timeout - Math.max(this.m_bucket.localPeer.RTT, remotePeer.RTT || 0);

        let [task, isNewEvent] = this.m_taskExecutor.emitBroadcastEvent(event,
                params,
                source,
                cmdPackage.body.taskid,
                {timeout});

        if (isNewEvent) {
            // 已经退出服务就不再发出事件通知了，但继续协助做完广播操作
            if (!this.m_bucket.localPeer.inactive) {
                let serviceDescriptor = this.m_bucket.localPeer.findService(this.m_servicePath);
                if (serviceDescriptor && serviceDescriptor.isSigninServer()) {
                    setImmediate(() => this.m_broadcastEventEmitter.emit(event, event, params, source));
                }
            }
        }
        task.process(cmdPackage, remotePeer);
    }

    _fillRecursionRespPackageBody(respPackage, cmdPackage, target, arrivedPeerids = null, eNodes = null, isDone = false) {
        let peerList = null;
        target = target || cmdPackage.body.target;
        if (this.m_bucket.localPeer.peerid !== target && !isDone) {
            if (!Array.isArray(eNodes)) {
                eNodes = null;
            }
            let eNodesSet =  new Set(eNodes || []);
            if (arrivedPeerids) {
                arrivedPeerids.forEach(peerid => eNodesSet.add(peerid));
            }
            // @var <array>
            if (target) {
                peerList = this.m_bucket.findClosestPeers(target, {excludePeerids: eNodesSet, maxDistance: this.m_bucket.distanceToLocal(target)});
            } else {
                peerList = this.m_bucket.getRandomPeers();
            }
            peerList = peerList.slice(0, Config.Bucket.ResponcePeerCount);
        }

        respPackage.common.ackSeq = cmdPackage.common.seq;

        respPackage.body = respPackage.body || {};
        respPackage.body.taskid = cmdPackage.body.taskid;
        if (peerList && peerList.length > 0) {
            respPackage.body.n_nodes = peerList.map(peer => {
                return {id: peer.peerid, eplist: [...peer.eplist]};
            });
        }

        if (arrivedPeerids && arrivedPeerids.size > 0) {
            respPackage.body.r_nodes = [];
            arrivedPeerids.forEach(peerid => respPackage.body.r_nodes.push(peerid));
        }
    }

    _processHoleCall(cmdPackage, remotePeer) {
        if (!cmdPackage.body.target || typeof cmdPackage.body.target.peerid !== 'string' || cmdPackage.body.target.peerid.length === 0) {
            return;
        }
        LOG_DEBUG(`LOCALPEER:(${this.m_bucket.localPeer.peerid}:${this.m_servicePath}) got hole call(to:${cmdPackage.body.target.peerid}) from peer(${cmdPackage.src.peerid})`);

        let targetPeerInfo = {peerid: cmdPackage.body.target.peerid, eplist: []};

        targetPeerInfo.eplist = cmdPackage.body.target.eplist || [];
        let targetPeer = this.m_bucket.findPeer(cmdPackage.body.target.peerid);
        if (targetPeer) {
            targetPeerInfo.eplist = Peer.unionEplist(targetPeerInfo.eplist, targetPeer.eplist);
        }
        
        let respPackage = this.m_packageFactory.createPackage(DHTCommandType.HOLE_CALL_RESP);
        respPackage.common.packageID = cmdPackage.common.packageID;
        respPackage.common.ackSeq = cmdPackage.common.seq;
        respPackage.body = {
            taskid: cmdPackage.body.taskid,
            target: targetPeerInfo,
        };
        this.m_packageSender.sendPackage(remotePeer, respPackage);

        if (targetPeer) {
            // 当前连接的地址可用于通信，但不可用于传播，不能写入respPackage
            let connectAddress = targetPeer.address;
            if (connectAddress) {
                targetPeerInfo.eplist = Peer.unionEplist(targetPeerInfo.eplist, [BaseUtil.EndPoint.toString(connectAddress)]);
            }
        }

        // 持有双方监听地址时才可以辅助穿透
        if (targetPeerInfo.eplist.length > 0 && remotePeer.eplist.length > 0) {
            this.m_taskExecutor.handshakeAgency(remotePeer, targetPeerInfo, cmdPackage.body.taskid);
        }
    }

    _processHoleCalled(cmdPackage, remotePeer) {
        if (!cmdPackage.body.src || typeof cmdPackage.body.src.peerid !== 'string' || cmdPackage.body.src.peerid.length === 0) {
            return;
        }
        LOG_DEBUG(`LOCALPEER:(${this.m_bucket.localPeer.peerid}:${this.m_servicePath}) got hole called(from:${cmdPackage.body.src.peerid}) from peer(${cmdPackage.src.peerid})`);

        let srcEPList = cmdPackage.body.src.eplist || [];
        let srcPeer = this.m_bucket.findPeer(cmdPackage.body.src.peerid);
        if (srcPeer) {
            srcEPList = Peer.unionEplist(srcEPList, srcPeer.eplist);
        }
        let srcPeerInfo = {peerid: cmdPackage.body.src.peerid, eplist: srcEPList};

        let respPackage = this.m_packageFactory.createPackage(DHTCommandType.HOLE_CALLED_RESP);
        respPackage.common.packageID = cmdPackage.common.packageID;
        respPackage.common.ackSeq = cmdPackage.common.seq;
        respPackage.body = {
            taskid: cmdPackage.body.taskid,
        };
        this.m_packageSender.sendPackage(remotePeer, respPackage);
        
        this.m_taskExecutor.handshakeTarget(srcPeerInfo, cmdPackage.body.taskid);
    }

    _processHandshake(cmdPackage, remotePeer) {
        LOG_DEBUG(`LOCALPEER:(${this.m_bucket.localPeer.peerid}:${this.m_servicePath}) got handshake from peer(${cmdPackage.src.peerid})`);
        let respPackage = this.m_packageFactory.createPackage(DHTCommandType.HANDSHAKE_RESP);
        respPackage.common.packageID = cmdPackage.common.packageID;
        respPackage.common.ackSeq = cmdPackage.common.seq;
        respPackage.body = {
            taskid: cmdPackage.body.taskid,
        };
        this.m_packageSender.sendPackage(remotePeer, respPackage);
        
        let task = this.m_taskMgr.getTaskByID(cmdPackage.body.taskid);
        if (task) {
            task.process(cmdPackage, remotePeer);
        }
    }
}

module.exports = PackageProcessor;