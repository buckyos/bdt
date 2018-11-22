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

"use strict";

const Base = require('../base/base.js');
const {P2P} = require('../index');
const DHTAPPID = require('../base/dhtappid');
const SERVICEID = require('../base/serviceid');

async function main(config) {
    let {result, p2p} = await P2P.create(config);
    if (result !== 0) {
        console.warn(`start sn(P2P.create) failed: result = ${result}`);
    } else {
        // 在发现的所有DHT网络中写入自己的SN信息；
        // 其他节点可以通过该DHT网络中的任何节点做为入口接入DHT，并在该DHT网络中搜索到SN信息
        if (config.asSeed) {
            p2p.on(P2P.EVENT.DHTCreate, dht => {
                if (dht.appid !== DHTAPPID.sn) {
                    dht.saveValue(SERVICEID.sn, config.peerid, p2p.dht.localPeer.eplist);
                }
            });
        }

        p2p.joinDHT([], {dhtAppID: DHTAPPID.sn, asDefault: true});
        result = p2p.startupSNService({minOnlineTime2JoinDHT: 0, joinDHTImmediately: true});
        if (result !== 0) {
            console.warn(`start sn(p2p.startupSNService) failed: result = ${result}`);
        }
        console.log(`sn (peerid=${config.peerid}) started at :${JSON.stringify(p2p.eplist)}`);

        // 聚合DHT入口
        p2p.startupSuperDHTEntry({autoJoin: true});

        // 定时更新数据
        if (config.asSeed) {
            setInterval(() => {
                p2p.getAllDHT().forEach(dht => {
                    if (dht.appid !== DHTAPPID.sn) {
                        dht.saveValue(SERVICEID.sn, peerid, dht.localPeer.eplist);
                    }
                });
            }, 600000);
        }
    }
}

Base.BX_SetLogLevel(Base.BLOG_LEVEL_OFF);

// 解释参数列表
let peerid = null;
let udpPort = null;
let tcpPort = null;
let asSeed = false;

function parseParams() {
    let params = process.argv.slice(2);
    let index = 0;
    while (index < params.length) {
        switch (params[index]) {
            case '-peerid':
                peerid = params[index + 1];
                index += 2;
                break;
            case '-udp':
                udpPort = params[index + 1];
                index += 2;
                break;
            case '-tcp':
                tcpPort = params[index + 1];
                index += 2;
                break;
            case '-asSeed':
                asSeed = params[index + 1];
                index += 2;
            default:
                index += 1;
                break;
        }
    }
}

parseParams();

let CONFIG = {
    peerid,
    asSeed,
};

if (tcpPort) {
    CONFIG.tcp = {
        addrList: ['0.0.0.0'],
        initPort: tcpPort,
        maxPortOffset: 0,
    };
}

if (udpPort) {
    CONFIG.udp = {
        addrList: ['0.0.0.0'],
        initPort: udpPort,
        maxPortOffset: 0,
    };
}

main(CONFIG);