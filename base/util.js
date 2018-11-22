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

const net = require('net');
const os = require('os');
const assert = require('assert');
const Crypto = require('crypto');

/**
 * Simple object check.
 * @param item
 * @returns {boolean}
 */
function isObject(item) {
    return (item && typeof item === 'object' && !Array.isArray(item));
}

const EndPoint = {
    PROTOCOL: {
        udp: 'u',
        tcp: 't',
    },

    FAMILY: {
        IPv4: 'IPv4',
        IPv6: 'IPv6',
    },

    CONST_IP: {
        zeroIPv4: ['0.0.0.0'],
        zeroIPv6: ['::', '0:0:0:0:0:0:0:0'],
        loopbackIPv4: ['127.0.0.1'],
        loopbackIPv6: ['::1', '0:0:0:0:0:0:0:1'],
    },

    toString(address, protocol) {
        let ipv = 0;
        if (address.family === EndPoint.FAMILY.IPv6) {
            ipv = 6;
        } else if (address.family === EndPoint.FAMILY.IPv4) {
            ipv = 4;
        }

        assert(protocol || address.protocol);
        return `${ipv}@${address.address}@${address.port}@${protocol || address.protocol || EndPoint.PROTOCOL.udp}`;
    },

    toAddress(epString) {
        let el = epString.split('@');
        if (el.length >= 4) {
            let addr = {};
            if (net.isIPv4(el[1])) {
                addr.family = EndPoint.FAMILY.IPv4;
            } else if (net.isIPv6(el[1])) {
                addr.family = EndPoint.FAMILY.IPv6;
            } else {
                if (el[0] === '4') {
                    addr.family = EndPoint.FAMILY.IPv4;
                } else if (el[0] === '6') {
                    addr.family = EndPoint.FAMILY.IPv6;
                }
            }
            addr.address = el[1];
            addr.port = parseInt(el[2]);
            addr.protocol = EndPoint.PROTOCOL.udp;
            addr.protocol = el[3];
            return addr;
        } else {
            return null;
        }        
    },

    isZero(address) {
        if (typeof address === 'string') {
            address = EndPoint.toAddress(address);
        }

        if (address.family === EndPoint.FAMILY.IPv4) {
            assert(EndPoint.CONST_IP.zeroIPv4.length === 1);
            return address.address === EndPoint.CONST_IP.zeroIPv4[0];
        } else {
            assert(EndPoint.CONST_IP.zeroIPv6.length === 2);
            return address.address === EndPoint.CONST_IP.zeroIPv6[0] || 
                address.address === EndPoint.CONST_IP.zeroIPv6[1];
        }
    },

    isLoopback(address) {
        if (typeof address === 'string') {
            const str = address;
            address = EndPoint.toAddress(str);
            assert(address, str);
        }

        if (address.family === EndPoint.FAMILY.IPv4) {
            assert(EndPoint.CONST_IP.loopbackIPv4.length === 1);
            return address.address === EndPoint.CONST_IP.loopbackIPv4[0];
        } else {
            assert(EndPoint.CONST_IP.loopbackIPv6.length === 2);
            return address.address === EndPoint.CONST_IP.loopbackIPv6[0] || address.address === EndPoint.CONST_IP.loopbackIPv6[1];
        }
    },

    isNAT(address) {
        if (typeof address === 'string') {
            address = EndPoint.toAddress(address);
        }

        if (EndPoint.isZero(address) || EndPoint.isLoopback(address)) {
            return true;
        }

        // 暂时认为IPv6地址都是公网地址
        if (!address.family || address.family == EndPoint.FAMILY.IPv4) {
            let el = address.address.split('.');
            if (el.length === 4) {
                let el1 = parseInt(el[1]);
                switch(el[0]) {
                    case '10': return true;
                    case '172': return el1 >= 0 && el1 <= 31;
                    case '192': return el1 === 168;
                }
            }
        }
        return false;
    },

    zero(family) {
        return family === EndPoint.FAMILY.IPv4? EndPoint.CONST_IP.zeroIPv4[0] : EndPoint.CONST_IP.zeroIPv6[0];
    },

    loopback(family) {
        return family === EndPoint.FAMILY.IPv4? EndPoint.CONST_IP.loopbackIPv4[0] : EndPoint.CONST_IP.loopbackIPv6[0];
    },

    // 猜测一个内网监听地址的公网访问地址
    // 如果一个endpoint的ip是0地址或者内网ip
    // 并且这个endpoint的协议是tcp协议,
    // 就返回一个判断结果和NAT的ip:port( 公网ip:声明时的监听端口 )
    // @return [bool 是否达成NAT条件, string 可能的公网访问endpoint]
    // 比如：监听TCP '0.0.0.0:1234'，并把端口映射到公网；从本地向外发起连接，通过对方反馈可发现自己的公网IP 'a.b.c.d'；
    // 这时可以猜测其他peer可能可以通过'a.b.c.d:1234'连接进来，当然也可能连不进来；
    // 同样的，也可能从本地对局域网内peer发起连接发现自己的局域网IP，然后猜测自己的地址。
    conjectureEndPoint(endpoint, internetAddress) {
        // make endpoint to be an object
        if ( typeof endpoint == 'string' ) {
            endpoint = EndPoint.toAddress(endpoint);
        } else if ( Array.isArray(endpoint) ) {
            endpoint = EndPoint.toAddress(endpoint.join('@'));
        }

        if (endpoint.family !== internetAddress.family) {
            return [false, ''];
        }
        
        const isNAT = EndPoint.isNAT(endpoint);
        const isZero = EndPoint.isZero(endpoint);
        const isTCP = EndPoint.PROTOCOL.tcp == endpoint.protocol;

        if ( ( isNAT || isZero ) &&  isTCP ) {
            let tcpListenerAddress = {
                family: internetAddress.family,
                address: internetAddress.address,
                port: endpoint.port,
                protocol: endpoint.protocol,
            };
            // 拼接公网ip和绑定端口
            const newEp = EndPoint.toString(tcpListenerAddress);
            return [true, newEp];
        }

        return [false, ''];
    }
};


// 高阶函数, 生成检查protocol的check函数
// @param 用作检查的类型
// @return function 检查函数
function generateProtocolCheck(protocol) {
    return function(address) {
        if ( address != null && typeof address === 'object' ) {
            // 如果address是对象,直接比较protocol即可
            return address.protocol === protocol;
        } else if ( typeof address === 'string' ) {
            // 如果address 是 'u' 或者 't'
            // 直接比较即可
            // 如果address 字符串形式的endpoint, 则需要转化
            if ( address.length == 1 ) {
                return address === protocol;
            } else {
                return EndPoint.toAddress(address).protocol === protocol;
            }
        }

        return false;
    }
}

EndPoint.isTCP = generateProtocolCheck(EndPoint.PROTOCOL.tcp);
EndPoint.isUDP = generateProtocolCheck(EndPoint.PROTOCOL.udp);

const NetHelper = {
    getLocalIPs(withInternal) {
        let ips = [];
        let netInterface = os.networkInterfaces();
        for (let name of Object.keys(netInterface)) {
            netInterface[name].forEach(info => {
                // 127.0.0.1和::1是回环地址，由参数指定是否过滤
                if (withInternal || !info.internal) {
                    if (info.family === EndPoint.FAMILY.IPv4) {
                        let el = info.address.split('.');
                        // 去掉0.x.x.x和169.254.x.x
                        if (el.length !== 4 ||
                            parseInt(el[0]) === 0 ||
                            (parseInt(el[0]) === 169 && parseInt(el[1]) === 254)) {
                            return;
                        }
                    } else if (info.family === EndPoint.FAMILY.IPv6) {
                        let el = info.address.split(':');
                        if (el.length === 0 ||
                            parseInt(el[0], 16) === 0xfe80) {
                            return;
                        }
                    }
                    ips.push(info.address);
                }
            });
        }
        return ips;
    },

    getLocalIPV4(withInternal) {
        return NetHelper.getLocalIPs(withInternal).filter(ip => net.isIPv4(ip))
    }
}

const algorithm = {
    /**
     * Deep merge two objects.
     * @param target
     * @param ...sources
     */
    mergeDeep(target, ...sources) {
        if (!sources.length) return target;
        const source = sources.shift();

        if (isObject(target) && isObject(source)) {
            for (const key in source) {
                if (isObject(source[key])) {
                    if (!target[key]) Object.assign(target, { [key]: {} });
                    algorithm.mergeDeep(target[key], source[key]);
                } else {
                    Object.assign(target, { [key]: source[key] });
                }
            }
        }

        return algorithm.mergeDeep(target, ...sources);
    },

    binarySearch(target, arr, compare, low, count) {
        if (!compare) {
            compare = (target, cursor) => target - cursor;
        }
        if (!low) {
            low = 0;
        }
        if (!count && count !== 0) {
            count = arr.length - low;
        }
        if (count < 0 || low < 0) {
            assert(false, `arg error:low=${low},count=${count}`);
            return [-1, -1];
        }

        if (count === 0) {
            return [-1, low];
        }

        const search = (low, high) => {
            if (low <= high) {
                let sub = compare(target, arr[low]);
                if (sub < 0) {
                    return [-1, low];
                } else if (sub === 0) {
                    return [low, low + 1];
                }
        
                sub = compare(target, arr[high]);
                if (sub > 0) {
                    return [-1, high + 1];
                } else if (sub === 0) {
                    return [high, high + 1];
                }
                
                let mid = (high + low) >> 1;
                sub = compare(target, arr[mid]);
                if (sub === 0) {
                    return [mid, mid + 1];
                } else if (sub > 0) {
                    return search(mid + 1, high);
                } else {
                    return search(low, mid - 1);
                }
            }
            return [-1, low];
        }
        return search(low, low + count - 1);
    },
    
    UInt(n) {
        return n >>> 0;
    }
}

const TimeHelper = {
    uptimeMS() {
        return Math.round(process.uptime() * 1000);
    },
}

/**
 * 用于计算两个KEY（STRING）之间距离的HASH
 * HASH长度暂时只取MD5的32位；便于计算，同时按距离分组时数量也不会太多
 * 考虑到以后可能需要扩展到更多位数，不可以把HASH值直接当作32位整数处理（如：比较，位运算等）；
 * 需要任何运算时要在HashDistance类中添加函数；
 * 如果真要扩展，已经使用的HASH应该放在最高位，或者直接顺着MD5值往后启用更多位，这样不会影响旧版本中两个距离远近比较和分组
 */
const HashDistance = {
    calcDistance(key1, key2) {
        return HashDistance.calcDistanceByHash(HashDistance.checkHash(key1), HashDistance.checkHash(key2));
    },

    hash(key, bitCount = HashDistance.HASH_BIT_COUNT) {
        assert(typeof key === 'string' && (bitCount === 32 || bitCount === 16), `The key(${key}) must be a string.and bitCount(${bitCount}) must be 16 or 32.`);

        let md5 = Crypto.createHash('md5');
        md5.update(key);
        let md5Hash = md5.digest();
        if (bitCount === 32) {
            let hash = md5Hash.readUInt32BE(0);
            return algorithm.UInt(hash & (~HashDistance.HIGH_BIT_MASK));
        } else {
            let hash = md5Hash.readUInt16BE(0);
            return algorithm.UInt(hash & (~HashDistance.HIGH_BIT_MASK16));
        }
    },
    
    checkHash(key) {
        if (typeof key === 'number') {
            return key;
        } else {
            return HashDistance.hash(key);
        }
    },

    calcDistanceByHash(hash1, hash2) {
        return algorithm.UInt(hash1 ^ hash2);
    },

    firstDifferentBit(hash1, hash2) {
        if (hash1 === hash2) {
            return HashDistance.HASH_BIT_COUNT;
        }

        let bits = 0;
        let xor = hash1 ^ hash2;
        let highBitMask = HashDistance.HIGH_BIT_MASK;
        while ((xor & (highBitMask >>> bits)) == 0) { // +-0
            bits++;
        }
        return bits;
    },

    hashBit(hash, bitPos, bitCount = 1) {
        // value << 32 == value?
        if (bitCount == 0 || bitPos >= HashDistance.HASH_BIT_COUNT) {
            return 0;
        }

        if (bitCount > HashDistance.HASH_BIT_COUNT - bitPos) {
            bitCount = HashDistance.HASH_BIT_COUNT - bitPos;
        }
        
        // mask = 0x1111111000000
        //                 |(bitCount)
        let mask = (HashDistance.HASH_MASK << (HashDistance.HASH_BIT_COUNT - bitCount));
        // mask = 0x0000111000000
        //              | |(bitPos+bitCount)
        //              |bitPos
        mask = (mask >>> bitPos);
        return algorithm.UInt(hash & mask);
    },

    or(hash1, hash2) {
        return algorithm.UInt(hash1 | hash2);
    },

    and(hash1, hash2) {
        return algorithm.UInt(hash1 & hash2);
    },

    xor(hash1, hash2) {
        return algorithm.UInt(hash1 ^ hash2);
    },

    moveRight(hash, bitCount) {
        return hash >>> bitCount;
    },

    moveLeft(hash, bitCount) {
        return algorithm.UInt(hash << bitCount);
    },

    max(hash1, hash2) {
        if (HashDistance.compareHash(hash1, hash2) >= 0) {
            return hash1;
        }
        return hash2;
    },

    min(hash1, hash2) {
        if (HashDistance.compareHash(hash1, hash2) <= 0) {
            return hash1;
        }
        return hash2;
    },

    isBitSet(hash, bitPos) {
        return !!HashDistance.hashBit(hash, bitPos);
    },

    // 仅仅用于在网络上得到两个理论上应该相等的hash值，这时候不检查最高位
    checkEqualHash(hash1, hash2) {
        if (typeof hash1 !== 'number' || typeof hash2 !== 'number') {
            return false;
        }
        return ((hash1 ^ hash2) & (~HashDistance.HIGH_BIT_MASK)) == 0; // +-0
    },

    compareHash(hash1, hash2) {
        return hash1 - hash2;
    },

    // 按照到targetHashObj距离从近到远排列
    sortByDistance(hashObjArray, targetHashObj) {
        let targetHash = targetHashObj.hash || HashDistance.checkHash(targetHashObj.peerid);
        hashObjArray.sort((obj1, obj2) => {
            let distance1 = HashDistance.calcDistance(targetHash, obj1.hash || HashDistance.checkHash(obj1.peerid));
            let distance2 = HashDistance.calcDistance(targetHash, obj2.hash || HashDistance.checkHash(obj2.peerid));
            let compare = HashDistance.compareHash(distance1, distance2);
            if (compare === 0) {
                if (!obj1.peerid) {
                    return 1;
                } else if (!obj2.peerid) {
                    return -1;
                }
                if (obj1.peerid > obj2.peerid) {
                    return 1;
                }
                return -1;
            }
            return compare;
        });

        // <TODO> 删除测试代码
        let lastDistance = 0;
        hashObjArray.forEach(obj => {
            let distance = HashDistance.calcDistance(targetHash, obj.hash || HashDistance.checkHash(obj.peerid));
            assert(distance >= lastDistance);
            lastDistance = distance;
        });
    },

    HASH_BIT_COUNT: 32,
    HASH_MASK: 0xFFFFFFFF,//((1 << HASH_BIT_COUNT) - 1);
    HIGH_BIT_MASK: 0x80000000,//(1 << (HASH_BIT_COUNT - 1));
    HIGH_BIT_MASK16: 0x8000,//(1 << (HASH_BIT_COUNT - 1));
    MAX_HASH: 0xFFFFFFFF, // HASH_MASK
};

// 32位序列号，解决序列号递增溢出问题，溢出后归0，并且在跟之前序列号比较时+0xFFFFFFFF还原
// 前置条件：不会短时间内产生溢出导致新序列号归0后再次追上旧的还生效的序列号
const SequenceU32 = {
    random() {
        return Math.floor(((Date.now() + Math.random() * 20160809) % 0xED89) * 32768);
    },

    compare(seq1, seq2) {
        return SequenceU32.delta(seq1, seq2);
    },

    add(seq, delta) {
        seq += delta;
        return seq % 0xFFFFFFFF;
    },

    sub(seq, delta) {
        seq -= delta;
        return seq < 0? seq + 0xFFFFFFFF : seq;
    },

    delta(seq1, seq2) {
        let delta = seq1 - seq2;
        if (delta > 0x80000000) {
            return delta - 0xFFFFFFFF;
        } else if (delta < -0x80000000) {
            return delta + 0xFFFFFFFF;
        }
        return delta;
    },
}

module.exports.EndPoint = EndPoint;
module.exports.NetHelper = NetHelper;
module.exports.algorithm = algorithm;
module.exports.TimeHelper = TimeHelper;
module.exports.HashDistance = HashDistance;
module.exports.SequenceU32 = SequenceU32;

if (require.main === module) {
    console.log(NetHelper.getLocalIPs(false));

    let arr = [{a:3}, {a:5}, {a:6}, {a:7}, {a:9}, {a:12}, {a:15}];
    let compare = (target, cursor) => target - cursor.a;
    console.log(algorithm.binarySearch(12, arr, compare));
    console.log(algorithm.binarySearch(15, arr, compare));
    console.log(algorithm.binarySearch(3, arr, compare));
    console.log(algorithm.binarySearch(1, arr, compare));
    console.log(algorithm.binarySearch(17, arr, compare));
}
