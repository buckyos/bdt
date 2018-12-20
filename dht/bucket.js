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
const {Config, HashDistance, RandomGenerator, EndPoint} = require('./util.js');
const Peer = require('./peer.js');
const assert = require('assert');
const BaseUtil = require('../base/util.js');
const TimeHelper = BaseUtil.TimeHelper;

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

const BucketConfig = Config.Bucket;
const HashConfig = Config.Hash;

let __totalBucketCount = 0;

class Bucket {
    constructor(localPeer,
        { appid = 0,
        BUCKET_COUNT = BucketConfig.BucketCount,
        BUCKET_SIZE = BucketConfig.BucketSize,
        TIMEOUT_MS = BucketConfig.PeerTimeoutMS } = {}) {

        this.BUCKET_COUNT = BUCKET_COUNT;
        this.BUCKET_SIZE = BUCKET_SIZE;
        this.TIMEOUT_MS = TIMEOUT_MS;
        if (localPeer instanceof Peer.LocalPeer) {
            this.m_localPeer = localPeer;
        } else {
            this.m_localPeer = new Peer.LocalPeer(localPeer);
        }

        // distance-mask:[1, 01, 001, 0001...0]
        this.m_buckets = [new SubBucket(this)];
        this.m_id = __totalBucketCount + 1;
        this.m_appid = appid;
        __totalBucketCount++;
    }

    get id() {
        return this.m_id;
    }

    activePeer(peer, isSent, isReceived, isTrust) {
        // 本地peer直接更新，不需要在这里更新，这里只支持把它加入到自己路由表中
        if (peer.peerid === this.m_localPeer.peerid) {
            //return {peer:this.m_localPeer.peerid, isNew: false, discard: false};
            peer = this.m_localPeer;
        }

        let now = TimeHelper.uptimeMS();
        if (isReceived && peer !== this.m_localPeer) {
            this.m_localPeer.lastRecvTime = now;
            if (peer.address && peer.address.protocol === EndPoint.PROTOCOL.udp) {
                this.m_localPeer.lastRecvTimeUDP = now;
            }
        }

        let discard = false;
        let replace = false;
        let hash = HashDistance.hash(peer.peerid);
        let {subBucket, index} = this._findBucket(hash);
        let {peer: peerObj, isNew} = subBucket.activePeer(peer, isSent, isReceived, isTrust);
        if (subBucket.peerList.length > this.BUCKET_SIZE) {
            let largeBucketItem = subBucket;
            if (index === this.m_buckets.length - 1 && this.m_buckets.length < this.BUCKET_COUNT) {
                let splitBitPos = this.m_buckets.length - 1;
                do {
                    let newSubBucket = largeBucketItem.split(splitBitPos, HashDistance.hashBit(this.m_localPeer.hash, splitBitPos));
                    if (newSubBucket) {
                        newSubBucket.m_indexInBucket = this.m_buckets.length;
                        this.m_buckets.push(newSubBucket);
                        LOG_ASSERT(newSubBucket.distRank === 0);
                        if (newSubBucket.peerList.length > this.BUCKET_SIZE) {
                            largeBucketItem = newSubBucket;
                            splitBitPos++;
                        } else {
                            break;
                        }
                    } else {
                        break;
                    }
                } while(this.m_buckets.length < this.BUCKET_COUNT);
            }

            if (largeBucketItem.peerList.length > this.BUCKET_SIZE) {
                largeBucketItem.knockOut();
                if (largeBucketItem.findPeer(peer.peerid)) {
                    replace = true;
                } else {
                    discard = true;
                }
            }
        }
        return {peer: peerObj, isNew, discard, replace };
    }
    
    get localPeer() {
        return this.m_localPeer;
    }

    get peerCount() {
        let count = 0;
        for (let item of this.m_buckets) {
            count += item.peerList.length;
        }
        return count;
    }

    get bucketCount() {
        return this.m_buckets.length;
    }

    distanceToLocal(peerid) {
        return HashDistance.calcDistance(this.m_localPeer.hash, peerid);
    }

    findClosestPeers(peerid, {excludePeerids = null, count = BucketConfig.FindPeerCount, maxDistance = HashDistance.MAX_HASH} = {}) {
        let hash = HashDistance.checkHash(peerid);
        let {subBucket, index} = this._findBucket(hash);
        let foundPeerList = subBucket.findClosestPeers(hash, {excludePeerids, count, maxDistance});

        for (let i = index + 1; foundPeerList.length < count && i < this.m_buckets.length; i++) {
            let peerList = this.m_buckets[i].findClosestPeers(hash, {excludePeerids, count: count - foundPeerList.length, maxDistance});
            foundPeerList.push(...peerList);
        }

        for (let i = index - 1; foundPeerList.length < count && i >= 0; i--) {
            let peerList = this.m_buckets[i].findClosestPeers(hash, {excludePeerids, count: count - foundPeerList.length, maxDistance});
            foundPeerList.push(...peerList);
        }
        return foundPeerList;
    }

    getRandomPeers({count = BucketConfig.FindPeerCount, excludePeerids = null} = {}) {
        let foundPeerList = [];
        let searchTimes = 0;
        let findCountOnce = 0;
        
        if (excludePeerids && !(excludePeerids instanceof Set)) {
            excludePeerids = new Set([...excludePeerids]);
        }

        // 把各个bucket中的peer交叉保存起来，一遍能返回不同bucket中的内容
        do {
            findCountOnce = 0;
            for (let bucket of this.m_buckets) {
                if (bucket.peerList.length > searchTimes) {
                    let peer = bucket.peerList[searchTimes];
                    if (!peer.isTimeout(this.TIMEOUT_MS)
                        && !(excludePeerids && excludePeerids.has(peer.peerid))) {

                        foundPeerList.push(peer);
                    }
                    findCountOnce++;
                }
            }
            searchTimes++;
        } while (findCountOnce > 0);

        if (foundPeerList.length > count) {
            let startPos = RandomGenerator.integer(foundPeerList.length - 1);
            let endPos = startPos + count - 1;
            if (endPos < foundPeerList.length) {
                foundPeerList.splice(0, startPos);
                foundPeerList.splice(endPos, foundPeerList.length - endPos);
            } else {
                foundPeerList.splice(endPos - foundPeerList.length, foundPeerList.length - count);
            }
        }

        return foundPeerList;
    }

    findPeer(peerid) {
        let hash = HashDistance.checkHash(peerid);
        let {subBucket} = this._findBucket(hash);
        return subBucket.findPeer(peerid);
    }

    removePeer(peerid) {
        let hash = HashDistance.checkHash(peerid);
        let {subBucket} = this._findBucket(hash);
        return subBucket.removePeer(peerid);
    }

    // 先只支持从远到近遍历
    forEachPeer(peerProcessor, nearFirst) {
        assert(!nearFirst);
        for (let bucket of this.m_buckets) {
            for (let peer of bucket.peerList) {
                peerProcessor(peer);
            }
        }
    }

    // 可扩展以容纳新的peer
    isExpandable(peerid) {
        let hash = HashDistance.checkHash(peerid);
        let {subBucket} = this._findBucket(hash);

        if (subBucket.findPeer(peerid)) {
            return false;
        }

        if (subBucket.distRank === 0 && this.m_buckets.length < this.BUCKET_COUNT) {
            return true;
        }

        return !subBucket.isFull;
    }

    findOwnerSubBucketOfPeer(peer) {
        if (peer.__ownerBucketMap) {
            let subBucket = peer.__ownerBucketMap.get(this.id);
            if (subBucket) {
                return subBucket;
            }
        }
        let ownedPeerObj = this.findPeer(peer.peerid);
        if (ownedPeerObj) {
            return ownedPeerObj.__ownerBucketMap.get(this.id);
        }
        return null;
    }

    _findBucket(hash) {
        let bucketPos = HashDistance.firstDifferentBit(hash, this.m_localPeer.hash);
        if (bucketPos >= this.m_buckets.length) {
            bucketPos = this.m_buckets.length - 1;
        }
        return {subBucket: this.m_buckets[bucketPos], index: bucketPos};
    }
    
    log() {
        let bucketNo = 0;
        let localPeerHash = this.m_localPeer.hash;
        for (let bucket of this.m_buckets) {
            LOG_DEBUG(`[DHT${this.m_appid}] mask:${HashDistance.hashBit(HashDistance.HASH_MASK, bucketNo)}, peers:`);
            for (let peer of bucket.peerList) {
                LOG_DEBUG(`[DHT${this.m_appid}] PEER(distanse:${HashDistance.calcDistance(peer.hash, localPeerHash)} = ${peer.hash} ^ ${localPeerHash})`);
            }

            bucketNo++;
        }
    }
}

class SubBucket {
    constructor(bucket, indexInBucket) {
        this.m_bucket = bucket;
        this.m_peerList = [];
        this.m_indexInBucket = indexInBucket || 0;
    }

    get peerList() {
        return this.m_peerList;
    }

    // 该桶中包含peer到本地peer距离的等级
    get distRank() {
        return this.m_bucket.bucketCount - this.m_indexInBucket - 1;
    }

    get isFull() {
        if (this.m_peerList.length < this.m_bucket.BUCKET_SIZE) {
            return false;
        } else {
            for (let peer of this.m_peerList) {
                if (peer.isTimeout(this.m_bucket.TIMEOUT_MS)) {
                    return false;
                }
            }
            return true;
        }
    }

    activePeer(peer, isSent, isReceived, isTrust) {
        const localPeer = this.m_bucket.localPeer;
        let isNew = false;
        let targetPeer = this.findPeer(peer.peerid);
        if (!targetPeer) {
            if (peer instanceof Peer.Peer) {
                targetPeer = peer;
            } else {
                targetPeer = new Peer.Peer(peer);
            }
            isNew = true;
            if (!targetPeer.__ownerBucketMap) {
                targetPeer.__ownerBucketMap = new Map();
            }
            targetPeer.__ownerBucketMap.set(this.m_bucket.id, this);
            this.m_peerList.push(targetPeer);
        } else if (targetPeer !== peer){
            if (isTrust) {
                // 所有信息都可信，全部全量更新一遍
                targetPeer.update(peer);
            } else {
                targetPeer.unionEplist(peer.eplist);
            }
        }

        let now = TimeHelper.uptimeMS();

        if (targetPeer !== localPeer) {
            if (isSent) {
                targetPeer.lastSendTime = now;
            }
    
            if (isReceived) {
                targetPeer.lastRecvTime = now;
                if (peer.address && peer.address.protocol === EndPoint.PROTOCOL.udp) {
                    targetPeer.lastRecvTimeUDP = now;
                }
            }
        }

        if (peer.address && isTrust) {
            // TCP当前通信地址只作为双方通信的地址，不能加入eplist用于传播
            if (peer.address.protocol === EndPoint.PROTOCOL.udp) {
                targetPeer.unionEplist([EndPoint.toString(peer.address)]);
            }

            if (peer.address) {
                let isAddressUpdate = !targetPeer.address || // 原PEER的address是空
                                    !targetPeer.isOnline(this.m_bucket.TIMEOUT_MS) || // 原PEER已经不在线
                                    !targetPeer.address.__recvTime || // 原PEER的address接收时间没设定
                                    peer.address.protocol === EndPoint.PROTOCOL.udp || // UDP直接覆盖
                                    (peer.address.protocol === EndPoint.PROTOCOL.tcp && // 优先用UDP，要用TCP替换UDP，必须等之前的UDP地址超时
                                        targetPeer.address.protocol === EndPoint.PROTOCOL.udp && 
                                        now - targetPeer.address.__recvTime >= Config.Package.Timeout);
                if (isAddressUpdate) {
                    targetPeer.address = peer.address;
                }
            }
        }

        return {peer: targetPeer, isNew};
    }

    split(splitBitPos, localPeerMaskBit) {
        // 这里不是必须的，对超时容忍多一点就带着这些超时节点拆分；
        // 严格一点就先剔除它们
        let limitSize = this.m_bucket.BUCKET_SIZE;
        let timeoutMS = this.m_bucket.TIMEOUT_MS;
        this.knockOut(false);

        if (this.m_peerList.length <= limitSize) {
            return null;
        }

        let newSubBucket = new SubBucket(this.m_bucket);

        let i = 0;
        do {
            let peer = this.m_peerList[i];
            if (HashDistance.hashBit(peer.hash, splitBitPos) === localPeerMaskBit) {
                peer.__ownerBucketMap.set(this.m_bucket.id, newSubBucket);
                newSubBucket.m_peerList.push(peer);
                this.m_peerList.splice(i, 1);
            } else {
                i++;
            }
        } while(i < this.m_peerList.length);

        // 被拆分过一次的bucket不再是本地peer所在的bucket了，不能再次被拆分，强行让这个bucket大小对齐
        this.knockOut(true);
        return newSubBucket;
    }

    knockOut(force = true) {
        let limitSize = this.m_bucket.BUCKET_SIZE;
        let timeoutMS = this.m_bucket.TIMEOUT_MS;
        if (this.m_peerList.length <= limitSize) {
            return;
        }
        
        // timeout
        for (let i = this.m_peerList.length - 1; i >= 0; i--) {
            let peer = this.m_peerList[i];
            if (peer.isTimeout(timeoutMS)) {
                this.m_peerList.splice(i, 1).forEach(timeoutPeer => timeoutPeer.__ownerBucketMap.delete(this.m_bucket.id));
                if (this.m_peerList.length <= limitSize) {
                    break;
                }
            }
        }

        if (force) {
            // 删除活跃时间最短的peer，活跃时间长的peer更大概率继续活跃
            while (this.m_peerList.length > limitSize) {
                this.m_peerList.pop().__ownerBucketMap.delete(this.m_bucket.id);
            }
        }
    }

    findClosestPeers(hash, {excludePeerids = null, count = BucketConfig.FindPeerCount, maxDistance = HashDistance.MAX_HASH} = {}) {
        LOG_ASSERT(count >= 0, `[DHT${this.m_bucket.m_appid}] Try find negative(${count}) peers.`);
        if (count < 0) {
            return [];
        }

        let foundPeerList = [];
        for (let curPeer of this.m_peerList) {
            if (curPeer.isTimeout(this.m_bucket.TIMEOUT_MS)) {
                continue;
            }
            let curPeerDistance = HashDistance.calcDistanceByHash(curPeer.hash, hash);
            if (HashDistance.compareHash(curPeerDistance, maxDistance) >= 0) {
                continue;
            }

            let farthestPeer = foundPeerList[foundPeerList.length - 1];
            if (foundPeerList.length < count
                || HashDistance.compareHash(curPeerDistance, HashDistance.calcDistanceByHash(farthestPeer.hash, hash)) < 0) {
                let done = false;
                for (let j = 0; j < foundPeerList.length; j++) {
                    if (HashDistance.compareHash(curPeerDistance, HashDistance.calcDistanceByHash(foundPeerList[j].hash, hash)) < 0
                        && !(excludePeerids && excludePeerids.has(curPeer.peerid))) {
                        foundPeerList.splice(j, 0, curPeer);
                        done = true;
                        if (foundPeerList.length > count) {
                            foundPeerList.pop();
                        }
                        break;
                    }
                }
                if (!done) {
                    foundPeerList.push(curPeer);
                }
            }
        }
        return foundPeerList;
    }

    findPeer(peerid) {
        for (let peer of this.m_peerList) {
            if (peer.peerid === peerid) {
                return peer;
            }
        }
        return null;
    }

    removePeer(peerid) {
        for (let i = 0; i < this.m_peerList.length; i++) {
            if (this.m_peerList[i].peerid === peerid) {
                this.m_peerList.splice(i, 1).forEach(removedPeer => removedPeer.__ownerBucketMap.delete(this.m_bucket.id));
                return;
            }
        }
    }
}

module.exports = Bucket;