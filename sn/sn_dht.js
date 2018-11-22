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

const {Config, Result: DHTResult, HashDistance, GetValueFlag} = require('../dht/util.js');
const EventEmitter = require('events');
const DHT = require('../dht/dht.js');
const DHTPeer = require('../dht/peer.js');
const assert = require('assert');
const HashConfig = Config.Hash;

const Base = require('../base/base.js');
const BaseUtil = require('../base/util.js');
const TimeHelper = BaseUtil.TimeHelper;

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

/*
    SN-SERVICE-INFO: {
        scope: maskBitCount, // 标识该SN服务节点范围（服务peer的PEERID-HASH值和该SN的PEERID-HASH值匹配位数）
    }
*/

const SN_PEER_COUNT = 5;
const SENSIOR_SN_COUNT = 1;
const DEFAULT_SERVICE_SCOPE = 10;

class SNDHT {
    constructor(_dht, {minOnlineTime2JoinDHT = 24 * 3600000, recentSNCacheTime = 120000, refreshSNDHTInterval = 600000} = {}) {
        this.m_dht = _dht;

        let localPeer = this.m_dht.localPeer;
        this.m_localPeer = {
            peerid: localPeer.peerid,
            hash: localPeer.hash,
        };

        this.m_eventEmitter = new EventEmitter();
        this.m_timerJoinService = null;
        this.m_isJoinedDHT = false;

        this.MINI_ONLINE_TIME_MS = minOnlineTime2JoinDHT;
        this.RECENT_SN_CACHE_TIME = recentSNCacheTime;
        this.REFRESH_SN_DHT_INTERVAL = refreshSNDHTInterval;

        this.m_snOnlineListener = null;
        this.m_recentSNMap = new Map(); // 近期上线SN

        this.m_actorFlag = 0;

        // <TODO>测试代码
        this.m_dht.__joinedDHTTimes = this.m_dht.__joinedDHTTimes || 0;
        this.m_dht.attachBroadcastEventListener(SNDHT.EVENT.SN.online, (eventName, params, sourcePeer) => {
            assert(eventName === SNDHT.EVENT.SN.online && params.peerid === sourcePeer.peerid,
                `eventName:${eventName},params:${JSON.stringify(params)},sourcePeer:${JSON.stringify(sourcePeer)}`);
            assert(this.m_dht.__joinedDHTTimes > 0, `joinTimes: ${this.m_dht.__joinedDHTTimes}, sourcePeer:${JSON.stringify(sourcePeer)},localPeer:${JSON.stringify(this.m_dht.m_bucket.localPeer.toStructForPackage())}`);            
        });
    }

    isRunning() {
        return (this.m_actorFlag !== 0);
    }

    signinVistor() {
        let isRunningBefore = this.isRunning();
        if (!isRunningBefore) {
            this._onStart();
        }
    }

    signoutVistor() {
        let isRunningBefore = this.isRunning();
        if (isRunningBefore && !this.isRunning()) {
            this._onStop();
        }
    }

    signinServer(immediately) {
        let isRunningBefore = this.isRunning();

        this._tryJoinService(immediately);
        if (!isRunningBefore) {
            this._onStart();
        }
    }

    signoutServer() {
        let isRunningBefore = this.isRunning();
        this.m_dht.activeLocalPeer(false);
        this._stopTryJoinService();
        if (isRunningBefore && !this.isRunning()) {
            this._onStop();
        }
    }

    findSN(peerid, fromCache, callback, onStep = undefined) {
        if (callback) {
            this._findSN(peerid,
                !fromCache,
                (result, snList) => callback({result, snList: (snList || [])}),
                (result, snList) => {
                    if (onStep) {
                        return onStep({result, snList: (snList || [])});
                    }
                });
        } else {
            return new Promise(resolve => {
                this._findSN(peerid,
                    !fromCache,
                    (result, snList) => resolve({result, snList: (snList || [])}),
                    (result, snList) => {
                        if (onStep) {
                            return onStep({result, snList: (snList || [])});
                        }
                    });
            });
        }
    }
        
    // SN服务端接口
    setServiceScope(maskBitCount) {
        this.m_dht.updateLocalPeerAdditionalInfo('scope', DEFAULT_SERVICE_SCOPE);
    }

    getServiceScope() {
        let maskBitCount = this.m_dht.getLocalPeerAdditionalInfo('scope');
        maskBitCount = maskBitCount || 0;
        return {maskBitCount};
    }

    get isJoinedDHT() {
        return this.m_isJoinedDHT;
    }

    attachEvent(eventName, listener) {
        if (typeof eventName === 'string' && typeof listener === 'function') {
            this.m_eventEmitter.on(eventName, listener);
            return {eventName, listener, result: DHTResult.SUCCESS};
        } else {
            LOG_ASSERT(false, `attachEvent invalid args type, (eventName type: ${typeof eventName}, listener type: ${typeof listener}).`);
            return {result: DHTResult.INVALID_ARGS};
        }
    }

    detachEvent(eventName, listener) {
        if (typeof eventName === 'string' && typeof listener === 'function') {
            this.m_eventEmitter.removeListener(eventName, listener);
            return DHTResult.SUCCESS;
        } else {
            LOG_ASSERT(false, `detachEvent invalid args type, (eventName type: ${typeof eventName}, listener type: ${typeof listener}).`);
            return DHTResult.INVALID_ARGS;
        }
    }

    emitBroadcastEvent(eventName, params) {
        return this.m_dht.emitBroadcastEvent(eventName, params);
    }

    // listener(eventName, params, sourcePeer)
    attachBroadcastEventListener(eventName, listener) {
        return this.m_dht.attachBroadcastEventListener(eventName, listener);
    }

    // attachBroadcastEventListener相同输入参数
    detachBroadcastEventListener(eventName, listener) {
        return this.m_dht.detachBroadcastEventListener(eventName, listener);
    }

    getNearSN(peerid, onlyRouteTable) {
        let peeridHash = HashDistance.hash(peerid);
        let nearestDistance = HashDistance.calcDistanceByHash(peeridHash, this.m_localPeer.hash);
        let nearestSN = this.m_localPeer;

        let findFromRecentOnline = () => {
            let timeoutSNs = [];
            let now = TimeHelper.uptimeMS();
            this.m_recentSNMap.forEach((snInfo, snPeerid) => {
                let cacheTime = now - snInfo.onlineTime;
                if (cacheTime > this.RECENT_SN_CACHE_TIME) {
                    timeoutSNs.push(snPeerid);
                    return;
                }
    
                let distance2SN = HashDistance.calcDistanceByHash(peeridHash, snInfo.hash);
                if (HashDistance.compareHash(distance2SN, nearestDistance) < 0) {
                    nearestDistance = distance2SN;
                    nearestSN = {peerid: snPeerid};
                }
            });
            
            timeoutSNs.forEach(snPeerid => this.m_recentSNMap.delete(snPeerid));
        }

        let findFromRouteTable = () => {
            let snList = this._filterNearSNList(this.m_dht.getAllOnlinePeers(), peeridHash);
            if (snList && snList.length > 0) {
                let snHash = (snList[0].hash || HashDistance.hash(snList[0].peerid));
                let distance2SN = HashDistance.calcDistanceByHash(peeridHash, snHash);
                if (HashDistance.compareHash(distance2SN, nearestDistance) < 0) {
                    nearestDistance = distance2SN;
                    nearestSN = snList[0];
                }
            }
        }
        
        if (!onlyRouteTable) {
            findFromRecentOnline();
        }
        findFromRouteTable();
        
        return nearestSN;
    }

    _onStart() {
    }

    _onStop() {
    }

    _getNearSNList(peerlist, targetHash, targetPeerid) {
        let normalSNList = [];
        let sensiorSNList = [];

        if (!peerlist || peerlist.length === 0) {
            return {normal: normalSNList, sensior: sensiorSNList};
        }

        HashDistance.sortByDistance(peerlist, {hash: targetHash, peerid: targetPeerid});
        let lastPeer = null;
        for (let peer of peerlist) {
            if (lastPeer && lastPeer.peerid === peer.peerid) {
                continue;
            }
            lastPeer = peer;

            let maskBitCount = 0;
            if (peer instanceof DHTPeer.Peer) {
                if (!peer.inactive) {
                    maskBitCount = peer.getAdditionalInfo('scope') || 0;
                }
            }

            if (HashDistance.firstDifferentBit(targetHash, peer.hash) >= maskBitCount) {
                if (HashDistance.isBitSet(peer.hash, 0)) {
                    sensiorSNList.push(peer);
                } else {
                    normalSNList.push(peer);
                }
            }
        }

        return {normal: normalSNList, sensior: sensiorSNList};
    }

    _findSN(peerid, forceSearch = false, callback = undefined, onStep = undefined) {
        if (peerid === this.m_localPeer.peerid && !forceSearch) {
            let snList = this._filterNearSNList(this.m_dht.getAllOnlinePeers(), this.m_localPeer.hash);
            if (snList.length >= SN_PEER_COUNT) {
                callback(DHTResult.SUCCESS, snList);
                return;
            }
        }

        let generateCallback = handle => ({result, peerlist}) => {
            if (!handle) {
                return;
            }                    
            let targetHash = HashDistance.checkHash(peerid);
            if (!peerlist) {
                peerlist = [];
            }
            peerlist = peerlist.concat(this.m_dht.getAllOnlinePeers());
            let snList = this._filterNearSNList(peerlist, targetHash);

            result = snList.length > 0? DHTResult.SUCCESS : DHTResult.FAILED;
            return handle(result, snList);
        }

        this.m_dht.findPeer(peerid, generateCallback(callback), generateCallback(onStep));
    }

    _tryJoinService(immediately) {
        if (this.m_timerJoinService) {
            return;
        }

        this.m_isJoinedDHT = false;
        let lastOnlineTime = 0; // 最后一次上公网的时间
        const MAX_DISTANCE_SERVICE_PEER = HashDistance.hashBit(HashDistance.MAX_HASH, DEFAULT_SERVICE_SCOPE, Config.Hash.BitCount);

        let limitTimes = 5;
        let recentState = [];
        let refreshState = localPeer => {
            let stat = DHT.stat();
            let nearDistance = this._getNearSNDistance();

            // 同范围内有其他SN在线，如果没上线则不上线，但是如果已经上线就维持在线
            let state = {
                distanceOk: nearDistance === 0 || HashDistance.compareHash(nearDistance, MAX_DISTANCE_SERVICE_PEER) > 0, // 距离范围内没有其他SN
                sentCount: stat.udp.send.pkgs + stat.tcp.send.pkgs,
                recvCount: stat.udp.recv.pkgs + stat.tcp.recv.pkgs,
                question: stat.udp.req + stat.tcp.req,
                answer: stat.udp.resp + stat.tcp.resp,
                RTT: localPeer.RTT,
            };

            if (recentState.length >= limitTimes) {
                recentState.shift();
            }
            recentState.push(state);
        }

        // 判定是否满足加入DHT的条件
        let canOnline = state => {
            return  state.distanceOk && // 范围内没有其他SN
                    state.sentCount && state.recvCount / state.sentCount > 0.95 && // 收包率
                    state.answer > 100 && state.question / state.answer > 0.95 && // QA比
                    state.RTT < 100; // 延迟
        }

        // 判定是否要退出DHT的SN服务；
        // 判定条件比加入条件宽松，减少上上下下的概率
        let needOffline = state => {
            return !state.sentCount || state.recvCount / state.sentCount < 0.90 ||
                    state.answer <= 100 || state.question / state.answer < 0.90 ||
                    state.RTT > 150;
        }

        let canJoinDHT = () => {
            if (recentState.length < limitTimes) {
                return false;
            }

            for (let state of recentState) {
                if (!canOnline(state)) {
                    return false;
                }
            }
            return true;
        }

        let needUnjoinDHT = () => {
            if (recentState.length < limitTimes) {
                return true;
            }

            let noOkCount = 0;
            for (let state of recentState) {
                if (!needOffline(state)) {
                    noOkCount++;
                }
            }
            return noOkCount / recentState.length >= 0.4;
        }

        let refresh = () => {
            let localPeer = this.m_dht.localPeer;
            refreshState(localPeer);
            
            let now = TimeHelper.uptimeMS();
            
            const onlineSNs = this.m_dht.getAllOnlinePeers();
            if ((immediately && (this.MINI_ONLINE_TIME_MS <= 0 || now - lastOnlineTime < this.MINI_ONLINE_TIME_MS)) // SN预热阶段，立即启动
                || (onlineSNs.length === 0 || onlineSNs.length === 1 && onlineSNs[0].peerid === this.m_localPeer.peerid)) {  // SN-DHT表中还是空的或者只有自己，立即启动
                this._joinDHT();
                return;
            }

            if (localPeer.natType === DHTPeer.NAT_TYPE.internet) {
                if (lastOnlineTime === 0) {
                    lastOnlineTime = now;
                }
                // 在internet上线足够久，并且附近没有SN上线
                LOG_DEBUG(`SN test online:now=${now},lastOnlineTime=${lastOnlineTime}, isJoined=${this.m_isJoinedDHT},nearSNDistance=${this._getNearSNDistance()}`);
                if (now - lastOnlineTime >= this.MINI_ONLINE_TIME_MS && canJoinDHT()) {
                    // SN上线；
                    if (limitTimes > 5) {
                        limitTimes -= 0.5;
                    }
                    this._joinDHT();
                } else if (needUnjoinDHT()) {
                    if (this.m_isJoinedDHT) {
                        limitTimes *= 2;
                        recentState = [];
                    }
                    this._unjoinDHT();
                }
            } else {
                LOG_DEBUG(`not internet.natType=${localPeer.natType}`);
                if (this.m_isJoinedDHT) {
                    limitTimes *= 2;
                    recentState = [];
                }
                lastOnlineTime = 0;
                this._unjoinDHT();
            }
        }

        this.m_timerJoinService = setInterval(refresh, this.REFRESH_SN_DHT_INTERVAL);
        // refresh();
    }

    _stopTryJoinService() {
        if (this.m_timerJoinService) {
            clearInterval(this.m_timerJoinService);
            this.m_timerJoinService = null;
        }
    }

    // SN上线；
    // 1.在SN子网中广播上线消息
    // 2.监听其他SN的上线消息，并通知合适的客户端在新SN上线，注意分流，考虑在客户端下次ping的时候随pingResp通知
    _joinDHT() {
        if (this.m_isJoinedDHT) {
            return;
        }

        this.m_dht.__joinedDHTTimes++;
        this.m_isJoinedDHT = true;
        this.m_dht.activeLocalPeer(true, {broadcast: false});
        //this.m_dht.updateLocalPeerAdditionalInfo('scope', DEFAULT_SERVICE_SCOPE);
        this._onStart();

        this.m_snOnlineListener = (eventName, params, sourcePeer) => {
            assert(eventName === SNDHT.EVENT.SN.online && params.peerid === sourcePeer.peerid,
                `eventName:${eventName},params:${JSON.stringify(params)},sourcePeer:${JSON.stringify(sourcePeer)}`);
            
            if (params.peerid === this.m_localPeer.peerid) {
                return;
            }
            this.m_recentSNMap.set(params.peerid, {onlineTime: TimeHelper.uptimeMS(), hash: HashDistance.hash(params.peerid)});
            this.m_dht.ping(sourcePeer);
            setImmediate(() => this.m_eventEmitter.emit(SNDHT.EVENT.SN.online, {peerid: params.peerid}));
        };
        this.m_dht.attachBroadcastEventListener(SNDHT.EVENT.SN.online, this.m_snOnlineListener);
        this.m_dht.emitBroadcastEvent(SNDHT.EVENT.SN.online, {peerid: this.m_localPeer.peerid});
    }

    _unjoinDHT() {
        if (!this.m_isJoinedDHT) {
            return;
        }

        this.m_isJoinedDHT = false;
        this.m_dht.activeLocalPeer(false);

        if (this.m_snOnlineListener) {
            this.m_dht.detachBroadcastEventListener(SNDHT.EVENT.SN.online, this.m_snOnlineListener);
            this.m_snOnlineListener = null;
        }
    }

    _filterNearSNList(snList, targetPeerHash) {
        let {normal: normalSNList, sensior: sensiorSNList} = this._getNearSNList(snList, targetPeerHash);
        let sensiorSNCount = Math.min(sensiorSNList.length, SENSIOR_SN_COUNT);
        let normalSNCount = Math.min(normalSNList.length, SN_PEER_COUNT - sensiorSNCount);
        normalSNList.splice(normalSNCount,
            normalSNList.length - normalSNCount,
            ...sensiorSNList.slice(0, sensiorSNCount));
        return normalSNList;
    }

    _getNearSNDistance() {
        let snList = this._filterNearSNList(this.m_dht.getAllOnlinePeers(), this.m_localPeer.hash);
        if (!snList || snList.length === 0) {
            return HashDistance.MAX_HASH;
        }

        let nearHash = snList[0].hash || HashDistance.hash(snList[0].peerid);
        return HashDistance.calcDistance(nearHash, this.m_localPeer.hash);    
    }
}

SNDHT.EVENT = {
    SN: {
        online: 'online', // SN上线
    }
};

SNDHT.ACTOR_FLAG = {
    visitor: 0x1,
    server: 0x1 << 1,
};

module.exports = SNDHT;