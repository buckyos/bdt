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

const {HashDistance, Config, EndPoint} = require('./util.js');
const PeerConfig = Config.Peer;
const ServiceDescriptor = require('./service_descriptor.js');

const Base = require('../base/base.js');
const BaseUtil = require('../base/util.js');
const TimeHelper = BaseUtil.TimeHelper;
const assert = require('assert');

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

class Peer {
    constructor({peerid, eplist, natType = NAT_TYPE.unknown, onlineDuration = 0, services = null, additionalInfo = null, hash = null, RTT = 0, inactive = false}) {
        let now = TimeHelper.uptimeMS();
        this.m_peerid = peerid;
        this.m_eplist = new Set(eplist || []);
        assert(typeof peerid === 'string' && peerid.length > 0, `${peerid}`);
        
        this._eraseZeroEP();
        this.m_additionalInfo = null;
        this._setAdditionalInfo(additionalInfo);
        this.m_address = null;
        this.m_lastRecvTime = 0;
        this.m_lastRecvTimeUDP = 0;
        this.m_lastSendTime = 0;
        this.m_lastSendTimeUDP = 0;
        let calcHash = HashDistance.hash(peerid);
        if (hash && HashDistance.checkEqualHash(hash, calcHash)) {
            this.m_hash = hash;
        } else {
            this.m_hash = calcHash;
        }
        if (inactive) {
            this.m_isInactive = inactive;
        }

        this.m_isIncome = false;
        this.m_servicesMgr = new ServiceDescriptor('', ServiceDescriptor.FLAGS_SIGNIN_SERVER, null);
        this.m_servicesMgr.updateServices(services);
        this.m_natType = natType;
        this.m_onlineTime = Math.ceil(now / 1000 - onlineDuration);
        this.m_rtt = RTT || Config.Package.InitRRT;
    }

    get peerid() {
        return this.m_peerid;
    }

    get hash() {
        return this.m_hash;
    }

    get eplist() {
        return [...this.m_eplist];
    }

    set eplist(newValue) {
        if (newValue !== this.m_eplist) {
            this.m_eplist = new Set(newValue);
            this._eraseZeroEP();
        }
    }

    get address() {
        return this.m_address;
    }

    set address(newValue) {
        // TCP当前通信地址只作为双方通信的地址，不能加入eplist用于传播
        if (newValue) {
            if (newValue.address && newValue.port && newValue.family) {
                this.m_address = Object.assign({}, newValue);

                if (newValue.protocol === EndPoint.PROTOCOL.udp) {
                    this.unionEplist([EndPoint.toString(newValue)]);
                }
            }
        } else {
            // 当前tcp通信地址在eplist中没有记录，不可随意删除
            if (this.m_address && this.m_address.protocol === EndPoint.PROTOCOL.udp) {
                this.m_address = null;
            }
        }
    }

    get natType() {
        return this.m_natType;
    }

    set natType(newValue) {
        this.m_natType = newValue;
    }

    get onlineDuration() {
        let duration = Math.ceil(TimeHelper.uptimeMS() / 1000 - this.m_onlineTime);
        return duration > 1? duration : 1;
    }

    update(peer) {
        if (peer.eplist && peer.eplist.length > 0) {
            this.eplist = peer.eplist;
        }
        
        this.updateServices(peer.services);
        this.additionalInfo = peer.additionalInfo;
        this.natType = peer.natType;
        this.m_onlineTime = peer.m_onlineTime;
    }

    unionEplist(eplist) {
        if (eplist) {
            for (let ep of eplist) {
                if (!EndPoint.isZero(ep)) {
                    this.m_eplist.add(ep);
                }
            }
        }
    }

    toStructForPackage() {
        let obj = {
            peerid: this.m_peerid,
            hash: this.m_hash,
            eplist: [...this.eplist],
        };
        
        return obj;
    }

    toStruct(eplist) {
        let obj = {
            peerid: this.m_peerid,
            hash: this.m_hash,
            eplist: eplist || this.eplist,
            natType: this.natType,
            onlineDuration: this.onlineDuration,
        };

        if (this.m_additionalInfo) {
            obj.additionalInfo = [...this.m_additionalInfo];
        }

        let servicesObj = this._servicesStruct();
        if (servicesObj) {
            obj.services = servicesObj.services;
        }

        return obj;
    }
    
    _servicesStruct() {
        let obj;
        if (!this.m_servicesMgr.services) {
            return obj;
        }
    
        obj = {
            services: [],
        };
        
        this.m_servicesMgr.services.forEach((desc, id) => {
            let subSvcObj = desc.toStructForPackage();
            subSvcObj.id = id;
            obj.services.push(subSvcObj);
        });
/*
        {
            LOG_DEBUG(`_servicesStruct(${this.peerid}) actived, service list:`);
            if (obj.services)
                obj.services.forEach((subSrv) => LOG_DEBUG(`ServiceID:${subSrv.id}, flags:${subSrv.flags}`));
        }
*/
        return obj;
    }
    
    get lastRecvTime() {
        return this.m_lastRecvTime;
    }

    set lastRecvTime(newValue) {
        this.m_lastRecvTime = newValue;
        if (!this.m_lastSendTime) {
            this.m_isIncome = true;
        }
    }
    
    get lastRecvTimeUDP() {
        return this.m_lastRecvTime;
    }

    set lastRecvTimeUDP(newValue) {
        this.m_lastRecvTimeUDP = newValue;
        if (!this.m_lastSendTime) {
            this.m_isIncome = true;
        }
    }

    get lastSendTime() {
        return this.m_lastSendTime;
    }

    set lastSendTime(newValue) {
        this.m_lastSendTime = newValue;
        // 避免第一次发包收到响应前被判定为超时
        if (!this.m_lastRecvTime) {
            this.m_lastRecvTime = newValue;
        }
    }

    get lastSendTimeUDP() {
        return this.m_lastSendTimeUDP;
    }

    set lastSendTimeUDP(newValue) {
        this.m_lastSendTimeUDP = newValue;
        // 避免第一次发包收到响应前被判定为超时
        if (!this.m_lastRecvTime) {
            this.m_lastRecvTime = newValue;
        }
    }
    
    get inactive() {
        return this.m_isInactive;
    }

    get additionalInfo() {
        return this.m_additionalInfo;
    }

    getAdditionalInfo(keyName) {
        return this.m_additionalInfo? this.m_additionalInfo.get(keyName) : undefined;
    }

    set additionalInfo(newValue) {
        this._setAdditionalInfo(newValue);
    }

    updateAdditionalInfo(keyName, newValue) {
        if (!this.m_additionalInfo) {
            this.m_additionalInfo = new Map();
        }
        this.m_additionalInfo.set(keyName, newValue);
    }

    deleteAdditionalInfo(keyName) {
        if (this.m_additionalInfo) {
            this.m_additionalInfo.delete(keyName);
            if (this.m_additionalInfo.size === 0) {
                this.m_additionalInfo = null;
            }
        }
    }

    findService(servicePath) {
        return this.m_servicesMgr.findService(servicePath);
    }
        
    getServiceInfo(servicePath, key) {
        return this.m_servicesMgr.getServiceInfo(servicePath, key);
    }

    /*
        SERVICE: {
                id: string,
                flags: int
                info: [[key,value]...],
                services: ARRAY[SERVICE],
            }
        services: ARRAY[SERVICE]
    */
    updateServices(services) {
        this.m_servicesMgr.updateServices(services);
    }
    
    // 一段时间内收到过包才认为它在线
    isOnline(limitMS) {
        return TimeHelper.uptimeMS() - this.m_lastRecvTime < limitMS;
    }

    // 发出包后一段时间内没有收到包认为它超时
    isTimeout(limitMS) {
        return this.m_lastSendTime - this.m_lastRecvTime > limitMS;
    }
    
    get services() {
        return this.m_servicesMgr.services;
    }

    get RTT() {
        return this.m_rtt;
    }

    set RTT(rtt) {
        this.m_rtt = rtt;
    }

    updateRTT(rtt) {
        let alpha = 0.125;
        this.m_rtt = rtt * 0.125 + this.m_rtt * (1 - 0.125);
    }

    static isValidPeerid(peerid) {
        return typeof peerid === 'string' && peerid.length > 0;
    }

    static unionEplist(eplist1, eplist2) {
        return [... new Set([...(eplist1 || []), ...(eplist2 || [])])];
    }

    static retryInterval(peer1, peer2) {
        peer1 = peer1 || {};
        peer2 = peer2 || {};
        let rtt = Math.max(peer1.RTT || 0, peer2.RTT || 0);
        let interval = rtt;
        if (rtt < Config.Package.RetryInterval) {
            interval = Math.max(rtt * 2, Config.Package.RetryInterval);
        } else {
            interval = Math.max(Config.Package.RetryInterval * 2, rtt * 1.2);
        }
        return Math.floor(interval);
    }

    _setAdditionalInfo(newValue) {
        if (!newValue) {
            this.m_additionalInfo = null;
        } else if (newValue !== this.m_additionalInfo) {
            this.m_additionalInfo = new Map([...newValue]);
        }
    }

    _eraseZeroEP() {
        // 只删除udp的0地址, 保留tcp
        let zeroEPList = [];
        for (let ep of this.m_eplist.keys()) {
            let addr = EndPoint.toAddress(ep);
            if (!addr ||
                (EndPoint.isZero(addr) && addr.protocol == EndPoint.PROTOCOL.udp)) {
                zeroEPList.push(ep);
            }
        }
        zeroEPList.forEach(ep => this.m_eplist.delete(ep));
    }
}

// 本地PEER负责维护自己的地址列表，定时更新当前有效地址
class LocalPeer extends Peer {
    constructor({peerid, eplist, services = null, additionalInfo = [], hash = null,
        EP_TIMEOUT = PeerConfig.epTimeout, SYM_EP_COUNT = PeerConfig.symEPCount, _eplistWithUpdateState = null, inactive = true}) {

        super({peerid, eplist: [], services, additionalInfo, hash, inactive});
        this.EP_TIMEOUT = EP_TIMEOUT;
        this.SYM_EP_COUNT = SYM_EP_COUNT;
        this.m_eplist = new Map();
        if (!(_eplistWithUpdateState instanceof Map)) {
            if (eplist) {
                for (let ep of eplist) {
                    this.m_eplist.set(ep, {isInitEP: true, updateTime: TimeHelper.uptimeMS()});
                }
                this._eraseZeroEP();
            }
            this.m_initEPCount = this.m_eplist.size;
        } else {
            this.m_initEPCount = 0;
            _eplistWithUpdateState.forEach((attr, ep) => {
                if (attr.isInitEP) {
                    this.m_initEPCount++;
                    this.m_eplist.set(ep, {isInitEP: true, updateTime: attr.updateTime});
                } else {
                    let info = {updateTime: attr.updateTime};
                    if (attr.isReuseListener) {
                        info.isReuseListener = true;
                    }
                    if (attr.isConjecture) {
                        info.isConjecture = true;
                    }
                    this.m_eplist.set(ep, info);
                }
            });
        }
        this.m_conjectureEPCount = 0; // 猜测地址数量
        this.m_discoverInternetEPCount = 0; // 发现公网地址数量

        this.m_questionCount = 0; // 收到其他peer询问包数量
        this.m_answerCount = 0; // 收到其他peer的回复包数量
    }

    get eplist() {
        let now = TimeHelper.uptimeMS();
        let validEpList = [];
        this.m_eplist.forEach((info, ep) => {
            if (info.isInitEP) {
                validEpList.push({ep, info});
            } else if (now - info.updateTime < this.EP_TIMEOUT){
                let addr = EndPoint.toAddress(ep);
                if (addr &&
                    (addr.protocol === EndPoint.PROTOCOL.udp || info.isReuseListener)) {

                    validEpList.push({ep, info});
                }
            }
        });

        validEpList.sort((a, b) => b.info.updateTime - a.info.updateTime);
        return validEpList.map(v => v.ep);
    }

    set eplist(newValue) {
        if (newValue !== this.m_eplist) {
            let eraseEPList = [];
            this.m_eplist.forEach((info, ep) => info.isInitEP? 0 : eraseEPList.push(ep));
            eraseEPList.forEach(ep => this.m_eplist.delete(ep));

            this.m_conjectureEPCount = 0;
            this.m_discoverInternetEPCount = 0;
            this.unionEplist(newValue);
        }
    }

    update(peer) {
        // 不处理
    }

    unionEplist(eplist, isReuseListener) {
        let now = TimeHelper.uptimeMS();

        let _unionEPList = (newEPList, _isReuseListener, isConjecture) => {
            for (let ep of newEPList) {
                let addr = EndPoint.toAddress(ep);
                if (!addr || EndPoint.isZero(addr)) {
                    continue;
                }
    
                if (!_isReuseListener) {
                    if (addr.protocol === EndPoint.PROTOCOL.tcp) {
                        continue;
                    }
                }

                let isNew = false;
                let isNAT = EndPoint.isNAT(addr);
                let info = this.m_eplist.get(ep);
                if (!info) {
                    isNew = true;
                    // 对称NAT，记录公网EP没有意义
                    if (_isReuseListener || isConjecture || isNAT || !this.isSymmetricNAT) {
                        info = {updateTime: now};
                        this.m_eplist.set(ep, info);

                        if (isConjecture) {
                            this.m_conjectureEPCount++;
                        } else {
                            if (!isNAT && addr.family === EndPoint.FAMILY.IPv4) {
                                this.m_discoverInternetEPCount++;
                            }
                        }
                    }
                } else {
                    info.updateTime = now;
                }
                // tcp需要区分是否是监听socket，监听socket可以传播出去，非监听socket传播出去没有意义，而且数量巨大
                if (info) {
                    if (_isReuseListener) {
                        info.isReuseListener = true;
                    }
                    if (isConjecture) {
                        if (isNew) {
                            info.isConjecture = true;
                        }
                    } else {
                        // 猜测的EP变成发现的EP
                        if (info.isConjecture) {
                            delete info.isConjecture;
                            this.m_conjectureEPCount--;
                            if (!isNAT && addr.family === EndPoint.FAMILY.IPv4) {
                                this.m_discoverInternetEPCount++;
                            }
                        }
                    }
                }
            }
        }

        // 如果一个endpoint的ip是0地址或者内网ip
        // 并且这个endpoint的协议是tcp协议,
        // 就需要做NAT转换
        let additionalEPList = [];
        if (!isReuseListener) {
            const outerAddress = BaseUtil.EndPoint.toAddress(eplist[0]);
            if (outerAddress) {
                this.m_eplist.forEach((info, ep) => {
                    // 不是初始声明的endpoint, 而且也不是复用连接的endpoint
                    if (!info.isInitEP && !info.isReuseListener) {
                        return
                    }
    
                    const [isOk, newEp] = BaseUtil.EndPoint.conjectureEndPoint(ep, outerAddress);
                    if (isOk) {
                        additionalEPList.push(newEp);
                    }
                });
            }
        }

        _unionEPList(eplist, isReuseListener, false);
        _unionEPList(additionalEPList, true, true);
        
        this._knockOut();
    }

    // 设置向特定EP发包的socket地址
    setSenderEP(ep, senderEP) {
        let epInfo = this.m_eplist.get(ep);
        if (epInfo) {
            epInfo.senderEP = senderEP;
        }
    }

    isTimeout() {
        return false;
    }

    active(enable = true) {
        if (enable) {
            this.m_isInactive = false;
            delete this.m_isInactive;
        } else {
            this.m_isInactive = true;
        }
    }

    setServiceInfo(servicePath, newValue) {
        let descriptor = findService(servicePath);
        if (descriptor) {
            descriptor.serviceInfo = newValue;
        }
    }

    signinService(servicePath) {
        this.m_servicesMgr.signinService(servicePath);
    }

    signoutService(servicePath) {
        this.m_servicesMgr.signoutService(servicePath);
    }
    
    updateServiceInfo(servicePath, key, value) {
        this.m_servicesMgr.updateServiceInfo(servicePath, key, value);
    }

    deleteServiceInfo(servicePath, key) {
        this.m_servicesMgr.deleteServiceInfo(servicePath, key);
    }

    toStructForPackage() {
        let listenEPList = [];
        let isSym = this.isSymmetricNAT;
        let lastIPV6s = [];
        let lastInitIPV6s = [];

        let addLastEP = (ep, info, list) => {
            for (let i = 0; i < list.length; i++) {
                let exist = list[i];
                if (exist.info.updateTime < info.updateTime) {
                    list.splice(i, 0, {ep, info});
                }
            }
            if (list.length > 2) {
                list.pop();
            }
        }

        this.m_eplist.forEach((info, ep) => {
            // 0地址传播没有意义
            let addr = EndPoint.toAddress(ep);
            if (addr && !EndPoint.isZero(addr)) {
                // IPV6有各种临时地址，只取最近用到的地址（公网发现地址和初始设定地址各两个）
                if (addr.family === EndPoint.FAMILY.IPv6) {
                    addLastEP(ep, info, info.isInitEP? lastInitIPV6s : lastIPV6s);
                } else if (!isSym || EndPoint.isNAT(addr)) {
                    // 对称NAT的公网EP分发出去没有意义，局域网地址可以碰碰运气
                    listenEPList.push(ep);
                }
            }
        });

        lastIPV6s.forEach(epInfo => listenEPList.push(epInfo.ep));
        lastInitIPV6s.forEach(epInfo => listenEPList.push(epInfo.ep));
        let obj = this.toStruct(listenEPList);
        if (this.m_isInactive) {
            obj.inactive = true;
        }
        return obj;
    }

    get isSymmetricNAT() {
        // 对于对称NAT中的节点，会从公网上发现很多不同的EP
        // 初始节点和猜测节点不算发现EP
        return this.m_discoverInternetEPCount > this.SYM_EP_COUNT;
    }

    get natType() {
        if (this.onlineDuration * 1000 < PeerConfig.NATTypeTime) {
            return NAT_TYPE.unknown;
        }

        if (this.isSymmetricNAT) {
            // LOG_DEBUG(`isSymmetricNAT:eplist.size=${this.m_eplist.size},initEPCount=${this.m_initEPCount},conjectureEPCount=${this.m_conjectureEPCount},internetEPCount:${this.m_discoverInternetEPCount}`);
            return NAT_TYPE.symmetricNAT;
        } else if (this.m_answerCount < 100 || this.m_questionCount / this.m_answerCount < 1) {
            // LOG_DEBUG(`restrictedNAT:this.m_answerCount=${this.m_answerCount},this.m_questionCount=${this.m_questionCount}`);
            return NAT_TYPE.restrictedNAT;
        } else {
            // 其他peer看到的地址都和发包采用地址相同，认为它有一个公网地址
            if (this.m_eplist) {
                let now = TimeHelper.uptimeMS();
                for (let [ep, epInfo] of this.m_eplist) {
                    // LOG_DEBUG(`now=${now},ep=${ep},epInfo=${JSON.stringify(epInfo)}`);
                    let epAddress = EndPoint.toAddress(ep);
                    if (epAddress &&
                        epInfo.senderEP && 
                        epAddress.family === EndPoint.FAMILY.IPv4 && 
                        (epInfo.isInitEP || now - epInfo.updateTime <= this.EP_TIMEOUT) && 
                        !EndPoint.isNAT(epAddress)) {

                        let epSenderAddress = EndPoint.toAddress(epInfo.senderEP);
                        // 收发地址完全匹配，或者发送地址是'0.0.0.0'但port匹配，刚好映射到相同port的情况时，会误判
                        if (epSenderAddress && 
                            (ep === epInfo.senderEP || (EndPoint.isZero(epSenderAddress) && epAddress.port === epSenderAddress.port))) {
                            return NAT_TYPE.internet;
                        }
                    }
                }
            }
        }
        return NAT_TYPE.NAT;
    }

    _knockOut() {
        let now = TimeHelper.uptimeMS();

        let outtimeEPList = [];
        let isNat = false;
        for (let [ep, info] of this.m_eplist) {
            if (info.isInitEP) {
                continue;
            }

            let outtimeCount = outtimeEPList.length;

            let addr = EndPoint.toAddress(ep);
            if (!addr || now - info.updateTime > this.EP_TIMEOUT) {
                outtimeEPList.push(ep);
            } else {
                if (addr.protocol === EndPoint.PROTOCOL.tcp && !info.isReuseListener) {
                    // TCP非监听地址只能用来通过收发地址的匹配性参考性识别NAT，保留无益
                    if (isNat || !info.senderEP) {
                        outtimeEPList.push(ep);
                    } else {
                        let senderAddress = EndPoint.toAddress(info.senderEP);
                        if (ep === info.senderEP || 
                            !senderAddress ||
                            (EndPoint.isZero(senderAddress) && addr.port === senderAddress.port)) {
                            outtimeEPList.push(ep);
                        } else {
                            isNat = true;
                        }
                    }
                }
            }

            if (outtimeEPList.length > outtimeCount) {
                if (info.isConjecture) {
                    this.m_conjectureEPCount--;
                } else if (addr && !EndPoint.isNAT(addr)) {
                    if (addr.family === EndPoint.FAMILY.IPv4) {
                        this.m_discoverInternetEPCount--;
                    }
                }
            }
        }

        outtimeEPList.forEach(ep => this.m_eplist.delete(ep));
    }

    get _eplistWithUpdateState() {
        return this.m_eplist;
    }

    onPackageRecved(isAnswer) {
        if (isAnswer) {
            this.m_answerCount++;
        } else {
            this.m_questionCount++;
        }
    }

    get answer() {
        return this.m_answerCount;
    }

    set answer(v) {
        this.m_answerCount = v;
    }

    get question() {
        return this.m_questionCount;
    }

    set question(v) {
        this.m_questionCount = v;
    }
}

const NAT_TYPE = {
    unknown: 0,
    internet: 1,
    NAT: 2,
    restrictedNAT: 3,
    symmetricNAT: 4,

    tostring(id) {
        switch(id) {
            case NAT_TYPE.internet: return 'internet';
            case NAT_TYPE.NAT: return 'NAT';
            case NAT_TYPE.restrictedNAT: return 'restrictedNAT';
            case NAT_TYPE.symmetricNAT: return 'symmetricNAT';
            default: return 'unknown';
        }
    },

    toID(strType) {
        switch(strType) {
            case 'internet' : return NAT_TYPE.internet;
            case 'NAT': return NAT_TYPE.NAT;
            case 'restrictedNAT': return NAT_TYPE.restrictedNAT;
            case 'symmetricNAT': return NAT_TYPE.symmetricNAT;
            default: return NAT_TYPE.unknown;
        }
    }
}

module.exports.Peer = Peer;
module.exports.LocalPeer = LocalPeer;
module.exports.NAT_TYPE = NAT_TYPE;