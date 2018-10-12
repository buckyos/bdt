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
const DHTPeer = require('../dht/peer.js');
const assert = require('assert');
const HashConfig = Config.Hash;
const SN_DHT_SERVICE_ID = '4243153e-8d98-4384-8ae0-0ca1802235e2';

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
    constructor(fatherDHT, {minOnlineTime2JoinDHT = 24 * 3600000, recentSNCacheTime = 120000, refreshSNDHTInterval = 600000} = {}) {
        this.m_fatherDHT = fatherDHT;
        this.m_snDHT = this.m_fatherDHT.prepareServiceDHT([SN_DHT_SERVICE_ID]);

        let localPeer = this.m_snDHT.localPeer;
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

        // <TODO>测试代码
        this.m_snDHT.__joinedDHTTimes = this.m_snDHT.__joinedDHTTimes || 0;
        this.m_snDHT.attachBroadcastEventListener(SNDHT.Event.SN.online, (eventName, params, sourcePeer) => {
            assert(eventName === SNDHT.Event.SN.online && params.peerid === sourcePeer.peerid,
                `eventName:${eventName},params:${JSON.stringify(params)},sourcePeer:${JSON.stringify(sourcePeer)}`);
            assert(this.m_snDHT.__joinedDHTTimes > 0, `sourcePeer:${JSON.stringify(sourcePeer)},localPeer:${JSON.stringify(this.m_fatherDHT.m_bucket.localPeer.toStructForPackage())}`);            
        });
    }

    signinVistor() {
        let isRunningBefore = this.m_snDHT.isRunning();
        this.m_snDHT.signinVistor();
        if (!isRunningBefore) {
            this._onStart();
        }
    }

    signoutVistor() {
        let isRunningBefore = this.m_snDHT.isRunning();
        this.m_snDHT.signoutVistor();
        if (isRunningBefore && !this.m_snDHT.isRunning()) {
            this._onStop();
        }
    }

    signinServer(isSeed, immediately) {
        let isRunningBefore = this.m_snDHT.isRunning();

        this._tryJoinService(isSeed, immediately);
        if (!isRunningBefore) {
            this._onStart();
        }
    }

    signoutServer() {
        let isRunningBefore = this.m_snDHT.isRunning();
        this.m_snDHT.signoutServer();
        this.m_fatherDHT.deleteValue(SN_DHT_SERVICE_ID, this.m_localPeer.peerid);
        this._stopTryJoinService();
        if (isRunningBefore && !this.m_snDHT.isRunning()) {
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
        this.m_snDHT.updateServiceInfo('scope', DEFAULT_SERVICE_SCOPE);
    }

    getServiceScope() {
        let maskBitCount = this.m_snDHT.getServiceInfo('scope');
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
        return this.m_snDHT.emitBroadcastEvent(eventName, params);
    }

    // listener(eventName, params, sourcePeer)
    attachBroadcastEventListener(eventName, listener) {
        return this.m_snDHT.attachBroadcastEventListener(eventName, listener);
    }

    // attachBroadcastEventListener相同输入参数
    detachBroadcastEventListener(eventName, listener) {
        return this.detachBroadcastEventListener(eventName, listener);
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
            let snList = this._filterNearSNList(this.m_snDHT.getAllOnlinePeers(), peeridHash);
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
        let servicePath = this.m_snDHT.servicePath;
        let lastPeer = null;
        for (let peer of peerlist) {
            if (lastPeer && lastPeer.peerid === peer.peerid) {
                continue;
            }
            lastPeer = peer;

            let maskBitCount = 0;
            if (peer instanceof DHTPeer.Peer) {
                let serviceDescriptor = peer.findService(servicePath);
                if (serviceDescriptor && serviceDescriptor.isSigninServer()) {
                    maskBitCount = serviceDescriptor.getServiceInfo([], 'scope') || 0;
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
            let snList = this._filterNearSNList(this.m_snDHT.getAllOnlinePeers(), this.m_localPeer.hash);
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
            peerlist = peerlist.concat(this.m_snDHT.getAllOnlinePeers());
            let snList = this._filterNearSNList(peerlist, targetHash);

            result = snList.length > 0? DHTResult.SUCCESS : DHTResult.FAILED;
            return handle(result, snList);
        }

        this.m_snDHT.findPeer(peerid, generateCallback(callback), generateCallback(onStep));
    }

    _tryJoinService(isSeed, immediately) {
        if (this.m_timerJoinService) {
            return;
        }

        this.m_isJoinedDHT = false;
        let lastOnlineTime = 0; // 最后一次上公网的时间
        const MAX_DISTANCE_SERVICE_PEER = HashDistance.hashBit(HashDistance.MAX_HASH, DEFAULT_SERVICE_SCOPE, Config.Hash.BitCount);

        let limitTimes = 5;
        let recentState = [];
        let refreshState = localPeer => {
            let stat = this.m_fatherDHT.stat().udp;
            let nearDistance = this._getNearSNDistance();

            // 同范围内有其他SN在线，如果没上线则不上线，但是如果已经上线就维持在线
            let state = {
                distanceOk: nearDistance === 0 || HashDistance.compareHash(nearDistance, MAX_DISTANCE_SERVICE_PEER) > 0, // 距离范围内没有其他SN
                sentCount: stat.send.pkgs,
                recvCount: stat.recv.pkgs,
                question: stat.req,
                answer: stat.resp,
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
                    state.sentCount && state.recvCount / state.sentCount > 1 && // 收包率
                    state.answer > 100 && state.question / state.answer > 1 && // QA比
                    state.RTT < 100; // 延迟
        }

        // 判定是否要退出DHT的SN服务；
        // 判定条件比加入条件宽松，减少上上下下的概率
        let needOffline = state => {
            return !state.sentCount || state.recvCount / state.sentCount < 0.98 ||
                    state.answer <= 100 || state.question / state.answer < 0.98 ||
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
            let localPeer = this.m_fatherDHT.localPeer;
            refreshState(localPeer);
            
            let now = TimeHelper.uptimeMS();
            
            if (immediately && now - lastOnlineTime < this.MINI_ONLINE_TIME_MS) {
                this._joinDHT(isSeed);
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
                    this._joinDHT(isSeed);
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
        refresh();
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
    _joinDHT(isSeed) {
        if (this.m_isJoinedDHT) {
            return;
        }

        this.m_snDHT.__joinedDHTTimes++;
        this.m_isJoinedDHT = true;
        this.m_snDHT.signinServer();
        //this.m_snDHT.updateServiceInfo('scope', DEFAULT_SERVICE_SCOPE);
        if (isSeed) {
            this.m_fatherDHT.saveValue(SN_DHT_SERVICE_ID, this.m_localPeer.peerid, this.m_fatherDHT.localPeer.eplist);
        }
        this._onStart();

        this.m_snOnlineListener = (eventName, params, sourcePeer) => {
            assert(eventName === SNDHT.Event.SN.online && params.peerid === sourcePeer.peerid,
                `eventName:${eventName},params:${JSON.stringify(params)},sourcePeer:${JSON.stringify(sourcePeer)}`);
            
            if (params.peerid === this.m_localPeer.peerid) {
                return;
            }
            this.m_recentSNMap.set(params.peerid, {onlineTime: TimeHelper.uptimeMS(), hash: HashDistance.hash(params.peerid)});
            this.m_fatherDHT.ping(sourcePeer);
            setImmediate(() => this.m_eventEmitter.emit(SNDHT.Event.SN.online, {peerid: params.peerid}));
        };
        this.m_snDHT.attachBroadcastEventListener(SNDHT.Event.SN.online, this.m_snOnlineListener);
        this.m_snDHT.emitBroadcastEvent(SNDHT.Event.SN.online, {peerid: this.m_localPeer.peerid});
    }

    _unjoinDHT() {
        if (!this.m_isJoinedDHT) {
            return;
        }

        this.m_isJoinedDHT = false;
        this.m_snDHT.signoutServer();
        this.m_fatherDHT.deleteValue(SN_DHT_SERVICE_ID, this.m_localPeer.peerid);

        if (this.m_snOnlineListener) {
            this.m_snDHT.detachBroadcastEventListener(SNDHT.Event.SN.online, this.m_snOnlineListener);
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
        let snList = this._filterNearSNList(this.m_snDHT.getAllOnlinePeers(), this.m_localPeer.hash);
        if (!snList || snList.length === 0) {
            return HashDistance.MAX_HASH;
        }

        let nearHash = snList[0].hash || HashDistance.hash(snList[0].peerid);
        return HashDistance.calcDistance(nearHash, this.m_localPeer.hash);    
    }
}

SNDHT.Event = {
    SN: {
        online: 'online', // SN上线
    }
};

module.exports = SNDHT;