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
const {Result: DHTResult, Config, HashDistance} = require('../util.js');
const Task = require('./task.js');
const DHTPackage = require('../packages/package.js');
const {ResendControlor} = require('../package_sender.js');
const {Peer} = require('../peer.js');
const Bucket = require('../bucket.js');
const DHTCommandType = DHTPackage.CommandType;
const BaseUtil = require('../../base/util.js');
const TimeHelper = BaseUtil.TimeHelper;

const LOG_TRACE = Base.BX_TRACE;
const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

const SaveValueConfig = Config.SaveValue;
const BroadcastConfig = Config.Broadcast;

// 广播顺序采用从远及近的顺序，因为自己和远距离节点的路由表重合度低，早期被收到重复广播包的概率小；
// 能及早地触达更大范围的节点
class BroadcastEventTask extends Task {
    constructor(owner, eventName, params, sourcePeer, taskid, {timeout = BroadcastConfig.TimeoutMS} = {}) {
        super(owner, {timeout});

        this.m_id = (taskid || Task.genGlobalTaskID(this.bucket.localPeer.peerid, this.m_id));
        this.m_eventName = eventName;
        this.m_params = params;
        this.m_sourcePeer = sourcePeer;
        this.m_package = null;

        this.m_pendingPeerMap = new Map();
        this.m_pendingPeerList = [];
        this.m_sendPeersCount = 0;

        // 这里用两个bucket记录广播已经抵达的peer；
        // 一个更密集记录距离localPeer较远的PEER，另一个更密集记录距离localPeer较近的PEER；
        // 密集记录远距离PEER是为了在收到远距离节点发来的广播包时能找到其附近的已触达节点，并告知它，避免再次发包；
        // 密集记录近距离PEER是因为：当收到近距离节点的广播请求包时，说明对方的远距离节点已经广播到位，需要告知它更近的触达节点，避免重复发包；
        // 两个bucket都是为了能找到对方附近的触达节点，并使该节点命中对方发送列表的概率更大
        this.m_arrivedFarBucket = null;
        this.m_arrivedNearBucket = new Bucket(this.bucket.localPeer);
    }
    
    _startImpl() {
        this.m_package = this._createPackage();

        let localPeerid = this.bucket.localPeer.peerid;
        let farPeer = null;
        // forEachPeer从远到近顺序返回，也从远到近开始广播
        this.bucket.forEachPeer(peer => {
            if (peer.peerid === this.m_sourcePeer.peerid ||
                peer.peerid === localPeerid) {
                return;
            }

            farPeer = farPeer || peer;

            let resender = new ResendControlor(peer,
                this.m_package,
                this.packageSender,
                Peer.retryInterval(this.bucket.localPeer, peer),
                Config.Package.RetryTimes,
                false);
            
            let peerEx = {
                resender,
                arrived: false,
            };
            this.m_pendingPeerMap.set(peer.peerid, peerEx);
            this.m_pendingPeerList.push(peerEx);
        });

        if (farPeer && this.bucket.bucketCount > this.bucket.BUCKET_COUNT / 3) {
            this.m_arrivedFarBucket = new Bucket(farPeer);
        }

        this._broadcast(Config.Broadcast.LimitPeerCountOnce);
    }

    // 即使全部广播到位也不完成，因为可能收到其他PEER转发过来的广播通知，任务留下来防止重复广播
    _processImpl(cmdPackage, remotePeer) {
        LOG_INFO(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) remotePeer:${cmdPackage.common.src.peerid} responsed BroadcastEventTask(${this.m_id})`);
        
        if (cmdPackage.cmdType === DHTCommandType.BROADCAST_EVENT_REQ) {
            // 因为是从远到近开始广播，所以收到的第一个请求包，来源peer应该距离比较远；
            if (!this.m_arrivedFarBucket) {
                this.m_arrivedFarBucket = new Bucket(remotePeer);
            }

            let respPackage = this.packageFactory.createPackage(DHTCommandType.BROADCAST_EVENT_RESP);
            respPackage.common.packageID = cmdPackage.common.packageID;
            respPackage.common.ackSeq = cmdPackage.common.seq;
            respPackage.body = {taskid: cmdPackage.body.taskid};
            if (cmdPackage.body.r_hashbits && typeof cmdPackage.body.r_hashbits === 'number') {
                let remotePeerHash = remotePeer.hash || HashDistance.hash(remotePeer.peerid);
                let mask = HashDistance.moveRight(HashDistance.HASH_MASK, cmdPackage.body.r_hashbits);
                let maxDistance = HashDistance.or(remotePeerHash, mask);
                maxDistance = HashDistance.min(maxDistance, HashDistance.calcDistanceByHash(remotePeerHash, this.bucket.localPeer.hash));

                let closePeers = new Map();
                let addClosePeers = bucket => {
                    let peers = bucket.findClosestPeers(remotePeer.peerid, {maxDistance});
                    if (peers && peers.length > 0) {
                        peers.forEach(peer => closePeers.set(peer.peerid, peer));
                    }
                }

                addClosePeers(this.m_arrivedFarBucket);
                addClosePeers(this.m_arrivedNearBucket);

                if (closePeers.size > 0) {
                    let closePeerArray = [...closePeers.values()];
                    HashDistance.sortByDistance(closePeerArray, remotePeer);
                    closePeerArray.splice(Config.Bucket.FindPeerCount, closePeerArray.length);
                    respPackage.body.r_nodes = closePeerArray.map(peer => peer.peerid);
                }
            }
            this.packageSender.sendPackage(remotePeer, respPackage);
        }

        let onPeerArrived = peer => {
            let peerEx = this.m_pendingPeerMap.get(peer.peerid);
            if (peerEx) {
                peerEx.arrived = true;
                this.m_pendingPeerMap.delete(peer.peerid);
            }

            if (this.m_arrivedFarBucket) {
                this.m_arrivedFarBucket.activePeer(peer);
            }
            this.m_arrivedNearBucket.activePeer(peer);
        }
        onPeerArrived(remotePeer);

        if (cmdPackage.cmdType === DHTCommandType.BROADCAST_EVENT_RESP &&
            cmdPackage.body.r_nodes && Array.isArray(cmdPackage.body.r_nodes)) {
                cmdPackage.body.r_nodes.forEach(peerid => {
                    if (typeof peerid === 'string' && peerid.length > 0) {
                        onPeerArrived({peerid, eplist: []})
                    }
                });
        }

        if (cmdPackage.cmdType === DHTCommandType.BROADCAST_EVENT_RESP) {
            // 一个包完成任务，离开网络，换两个包进入网络
            // 丢包率在50%以内可控制该广播任务有越来越多的包进入网络，不间断地执行；受bucket容量限制，网络中的包也不会无限制增大；
            // 若超过50%，可能执行中的包会越来越少，最后要靠超时来驱动
            this._broadcast(2);
        } else {
            // 任务已经开始了，这个请求包其实是重复的，往网络上多添一个包驱动一下
            this._broadcast(1);
        }
    }

    _retryImpl() {
        this._broadcast(Config.Broadcast.LimitPeerCountOnce);
    }
        
    _onCompleteImpl(result) {
    }

    _createPackage() {
        let cmdPackage = this.packageFactory.createPackage(DHTCommandType.BROADCAST_EVENT_REQ);

        cmdPackage.body = {
            taskid: this.m_id,
            event: this.m_eventName,
            source: {peerid: this.m_sourcePeer.peerid, eplist: this.m_sourcePeer.eplist},
            timeout: Math.max(this.deadline - TimeHelper.uptimeMS(), 809),
            r_hashbits: (Config.Bucket.BucketCount >>> 2), // 对方返回触达节点范围太大很难命中本地发送列表
        };

        if (this.servicePath && this.servicePath.length > 0) {
            cmdPackage.body.servicePath = this.servicePath;
        }

        if (this.m_params !== undefined && this.m_params !== null) {
            cmdPackage.body.params = this.m_params;
        }
        return cmdPackage;
    }

    _stopImpl() {
    }

    _broadcast(limitCount) {
        this.m_package.body.timeout = Math.max(this.deadline - TimeHelper.uptimeMS(), 809);

        let sendCount = 0;
        let timeoutPeerids = [];
        for (let i = 0; i < this.m_pendingPeerList.length;) {
            let peerEx = this.m_pendingPeerList[i];
            if (peerEx.arrived) {
                peerEx.resender.finish();
                this.m_pendingPeerList.splice(i, 1);
                continue;
            }

            let tryTimes = peerEx.resender.tryTimes;
            peerEx.resender.send();
            if (peerEx.resender.tryTimes !== tryTimes) {
                sendCount++;
                if (peerEx.resender.tryTimes === 1) {
                    this.m_sendPeersCount++;
                }
            }
            if (peerEx.resender.isTimeout()) {
                this.m_pendingPeerList.splice(i, 1);
                timeoutPeerids.push(peerEx.resender.peer.peerid);
            } else {
                i++;
            }
            if (sendCount >= limitCount) {
                break;
            }
        }

        timeoutPeerids.forEach(peerid => this.m_pendingPeerMap.delete(peerid));

        // 每8个包要求对方返回命中节点的范围缩小1
        this.m_package.body.r_hashbits = (Config.Bucket.BucketCount >>> 2) + (this.m_sendPeersCount >>> 3);
        if (this.m_package.body.r_hashbits > Config.Bucket.BucketCount - 2) {
            this.m_package.body.r_hashbits = Config.Bucket.BucketCount - 2;
        }
    }
}

module.exports = BroadcastEventTask;