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
const BDT = require('../bdt/bdt.js');
const { EndPoint, TimeHelper } = require('../base/util')

class RemoteBlackList {
    constructor() {
        this.m_forbiddenIPs = new Map();
    }

    /**
     * 禁用PEERID|IP地址
     * @param {string|RemoteBlackList.FORBID.all} peerid 禁用的PEERID，undefined/null等代表逻辑false的值都被解释为P2P.FORBID.all，禁用指定IP上的所有peerid
     * @param {string|Connection|EndPoint{family@ip@port@protocol}|{address:ip}|RemoteBlackList.FORBID.all} objectWithIP 禁用的IP，undefined/null等同peerid一样处理
     * @param { object{timeout} } options 
     */
    forbid(objectWithIP, peerid, options) {
        peerid = peerid || RemoteBlackList.FORBID.all;
        const ip = RemoteBlackList._getIP(objectWithIP);
        if (ip === RemoteBlackList.FORBID.all &&
            peerid === RemoteBlackList.FORBID.all) {
            return;
        }
        
        const now = TimeHelper.uptimeMS();
        let info = {
            deadline: now + RemoteBlackList.FORBID.timeout,
        }

        if (options && (typeof options.timeout) === 'number') {
            info.deadline = now + options.timeout;
        }

        let forbidPeerids = this.m_forbiddenIPs.get(ip);
        if (!forbidPeerids) {
            forbidPeerids = new Map();
            this.m_forbiddenIPs.set(ip, forbidPeerids);
        }
        forbidPeerids.set(peerid, info);
    }

    /**
     * 查询指定peerid|ip是否被禁用
     * @param {string|RemoteBlackList.FORBID.all} peerid 禁用的PEERID，undefined/null等代表逻辑false的值都被解释为P2P.FORBID.all，禁用指定IP上的所有peerid
     * @param {string|Connection|EndPoint{family@ip@port@protocol}|{address:ip}|RemoteBlackList.FORBID.all} objectWithIP 禁用的IP，undefined/null等同peerid一样处理
     * @return {boolean}
     */
    isForbidden(objectWithIP, peerid) {
        peerid = peerid || RemoteBlackList.FORBID.all;
        const ip = RemoteBlackList._getIP(objectWithIP);
        if (ip === RemoteBlackList.FORBID.all &&
            peerid === RemoteBlackList.FORBID.all) {
            return false;
        }

        const now = TimeHelper.uptimeMS();
        
        const isIPForbidden = ip => {
            let forbidPeerids = this.m_forbiddenIPs.get(ip);
            if (!forbidPeerids) {
                return false;
            }

            const isPeeridForbidden = peerid => {
                const info = forbidPeerids.get(peerid);
                if (!info) {
                    return false;
                }
                // 超时清理
                if (info.deadline < now) {
                    forbidPeerids.delete(peerid);
                    if (forbidPeerids.size === 0) {
                        this.m_forbiddenIPs.delete(ip);
                    }
                    return false;
                }
                return true;
            }

            return (ip !== RemoteBlackList.FORBID.all && isPeeridForbidden(RemoteBlackList.FORBID.all)) ||
                (peerid !== RemoteBlackList.FORBID.all && isPeeridForbidden(peerid));
        }

        return isIPForbidden(RemoteBlackList.FORBID.all) ||
            (ip !== RemoteBlackList.FORBID.all && isIPForbidden(ip));
    }

    static _getIP(objectWithIP) {
        if (!objectWithIP || objectWithIP === RemoteBlackList.FORBID.all) {
            return RemoteBlackList.FORBID.all;
        }

        if (objectWithIP instanceof BDT.Connection) {
            if (objectWithIP.remote.endpoint) {
                const addr = EndPoint.toAddress(objectWithIP.remote.endpoint);
                if (addr && addr.address) {
                    return addr.address;
                }
            }
            return null;
        }

        switch (typeof objectWithIP) {
            case 'object': return objectWithIP.address;
            case 'string': {
                const addr = EndPoint.toAddress(objectWithIP);
                if (addr) {
                    return addr.address;
                } else {
                    return objectWithIP;
                }
            }
            default:
                return null;
        }
    }
}

RemoteBlackList.FORBID = {
    timeout: 600809,
    all: Symbol(),
};

module.exports = RemoteBlackList;