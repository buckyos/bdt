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

// 简单的dht节点测试程序
'use strict';

const os = require("os");
const dgram = require('dgram');
const DHT = require('../../dht/dht.js');
const DHTUtil = require('../../dht/util.js');
const CrashListener = require("../crash_listener.js");
const P2P = require('../../p2p/p2p');

const path = require("path");
const Base = require('../../base/base.js');
const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

class DHTPeer {
    constructor(peerinfo) {
        this.m_dht = null;
        this.m_peerinfo = peerinfo;
        this.m_broadcastEventListener = null;
    }

    async start() {
        let {result, p2p} = await P2P.create({
            peerid: this.m_peerinfo.peerid,
            udp: {
                addrList:[this.m_peerinfo.dev.ip],
                initPort: this.m_peerinfo.port,
                maxPortOffset: 1,
            },
        });

        if (result !== 0) {
            LOG_WARN(`SN service listening failed.`);
            return;
        }

        p2p.joinDHT([this.m_peerinfo.seedDHTNode]);
        this.m_dht = p2p.dht;
    }

    findPeer(peerid) {
        // this.m_dht.findPeer(peerid, (result, peerlist) => {
        //     console.log(`find PEER(${peerid}) done, result = ${result}, find count is ${peerlist? peerlist.length : 0}`);
        // }, null);
        this.m_dht.findPeer(peerid)
            .then(({result, peerlist}) => {
                console.log(`PEER<${this.m_peerinfo.peerid}>: find PEER(${peerid}) done, find count is ${peerlist? peerlist.length : 0}`);
            })
            .catch(result => console.log(`PEER<${this.m_peerinfo.peerid}>: find PEER(${peerid}) failed, reason = ${result}`));
    }

    saveValue(tableName, keyName, value) {
        this.m_dht.saveValue(tableName, keyName, value);
        console.log(`saveValue(${tableName}:${keyName}:${value}) done.`);
    }

    getValue(tableName, keyName, flags) {
        // this.m_dht.getValue(tableName, keyName, flags, ({result, values}) => {
        //     console.log(`PEER<${this.m_peerinfo.peerid}>: getValue(${tableName}:${keyName}:${flags}) done, result = ${result}, value count is ${values? values.size : 0}:`);
        //     if (values) {
        //         values.forEach((value, key) => console.log(`key:${key}, value:${value}`));
        //     }
        // });
        this.m_dht.getValue(tableName, keyName, flags)
            .then(({result, values}) => {
                console.log(`PEER<${this.m_peerinfo.peerid}>: getValue(${tableName}:${keyName}:${flags}) done, value count is ${values? values.size : 0}:`);
                if (values) {
                    values.forEach((value, key) => console.log(`key:${key}, value:${value}`));
                }
            })
            .catch(result => console.log(`PEER<${this.m_peerinfo.peerid}>: getValue(${tableName}:${keyName}:${flags}) failed, reason = ${result}`));
    }

    emitBroadcastEvent(eventName, params) {
        // this.m_dht.emitBroadcastEvent(eventName, params, peerCount, ({result, arrivedCount}) => {
        //     console.log(`PEER<${this.m_peerinfo.peerid}>: emitBroadcastEvent(${eventName}) done, result = ${result}, arrivedCount is ${arrivedCount}.`);
        // });
        this.m_dht.emitBroadcastEvent(eventName, params);
            // .then(({result, arrivedCount}) => {
            //     console.log(`PEER<${this.m_peerinfo.peerid}>: emitBroadcastEvent(${eventName}) done, arrivedCount is ${arrivedCount}.`);
            // })
            // .catch(result => console.log(`PEER<${this.m_peerinfo.peerid}>: emitBroadcastEvent(${eventName}) done, reason = ${result}.`));
    }

    attachBroadcastEvent(eventName) {
        this.m_broadcastEventListener = (eventName, params, sourcePeer) => {
            console.log(`PEER<${this.m_peerinfo.peerid}>: Event arrive(name:${eventName}, params: ${params})`);
        }
        this.m_dht.attachBroadcastEventListener(eventName, this.m_broadcastEventListener);
    }

    detachBroadcastEvent(eventName) {
        this.m_dht.detachBroadcastEventListener(eventName, this.m_broadcastEventListener);
        console.log(`PEER<${this.m_peerinfo.peerid}>: detachBroadcastEventListener(${eventName})`);
    }

    static run(peerinfo) {
        let dhtPeer = new DHTPeer(peerinfo);
        dhtPeer.start();
    }
}

if (require.main === module) {
    let crashListener = new CrashListener();
    crashListener.listen();

    let args = process.argv.slice(2);
    let peerinfo = JSON.parse(args[0]);

    let logFolder;
    if (os.platform() === 'win32') {
        logFolder = "D:\\blog\\";
    } else {
        logFolder = "/var/blog/";
        Base.BX_SetLogLevel(Base.BLOG_LEVEL_WARN);
    }
    crashListener.enableFileLog(logFolder);
    Base.BX_SetLogLevel(Base.BLOG_LEVEL_WARN);
    Base.BX_EnableFileLog(logFolder, `${path.basename(require.main.filename, ".js")}-${peerinfo.port}`, '.log');
    Base.blog.enableConsoleTarget(false);

    DHTPeer.run(peerinfo);

    // <TODO> fill 测试数据
}

module.exports = DHTPeer;