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

const dgram = require('dgram');
const DHT = require('../../dht/dht.js');
const DHTUtil = require('../../dht/util.js');
const P2P = require('../../p2p/p2p.js');

const Base = require('../../base/base.js');
const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

Base.BX_SetLogLevel(Base.BLOG_LEVEL_WARN);

const TEST_ENABLE_CONFIG = {
    SET_GET_VALUE: false,
    BROADCAST: true,
    SUB_SERVICE: true,
};
const LOCAL_IP = '192.168.100.175';
const SN_SERVICE_ID = 'SN-SERVICE_ID';
const BROADCAST_EVENT_NAME = 'BROADCAST_EVENT_NAME';
const SN_BROADCAST_EVENT_NAME = `${BROADCAST_EVENT_NAME}:${SN_SERVICE_ID}`;

const SuperNodeInfoList = [
    {
        peerid: 'SN-1',
        address: '127.0.0.1',
        port: 1034,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@1034@u', `4@${LOCAL_IP}@1034@u`],
    },
    {
        peerid: 'SN-2',
        address: '0.0.0.0',
        port: 1035,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@1035@u', `4@${LOCAL_IP}@1035@u`],
    },
    {
        peerid: 'SN-3',
        address: '0.0.0.0',
        port: 1036,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@1036@u', `4@${LOCAL_IP}@1036@u`],
    },
    {
        peerid: 'SN-4',
        address: '127.0.0.1',
        port: 1037,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@1037@u', `4@${LOCAL_IP}@1037@u`],
    },
];

const NormalNodeInfoList = [
    {
        peerid: 'NORMAL-1',
        address: '127.0.0.1',
        port: 2034,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2034@u', `4@${LOCAL_IP}@2034@u`],
    },
    {
        peerid: 'NORMAL-2',
        address: '0.0.0.0',
        port: 2035,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2035@u', `4@${LOCAL_IP}@2035@u`],
    },
    {
        peerid: 'NORMAL-3',
        address: '0.0.0.0',
        port: 2036,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2036@u', `4@${LOCAL_IP}@2036@u`],
    },
    {
        peerid: 'NORMAL-4',
        address: '127.0.0.1',
        port: 2037,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2037@u', `4@${LOCAL_IP}@2037@u`],
    },
    {
        peerid: 'NORMAL-5',
        address: '127.0.0.1',
        port: 2038,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2038@u', `4@${LOCAL_IP}@2038@u`],
    },
    {
        peerid: 'NORMAL-6',
        address: '0.0.0.0',
        port: 2039,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2039@u', `4@${LOCAL_IP}@2039@u`],
    },
    {
        peerid: 'NORMAL-7',
        address: '0.0.0.0',
        port: 2040,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2040@u', `4@${LOCAL_IP}@2040@u`],
    },
    {
        peerid: 'NORMAL-8',
        address: '127.0.0.1',
        port: 2041,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2041@u', `4@${LOCAL_IP}@2041@u`],
    },
    {
        peerid: 'NORMAL-9',
        address: '127.0.0.1',
        port: 2042,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2042@u', `4@${LOCAL_IP}@2042@u`],
    },
    {
        peerid: 'NORMAL-10',
        address: '0.0.0.0',
        port: 2043,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2043@u', `4@${LOCAL_IP}@2043@u`],
    },
    {
        peerid: 'NORMAL-11',
        address: '0.0.0.0',
        port: 2044,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2044@u', `4@${LOCAL_IP}@2044@u`],
    },
    {
        peerid: 'NORMAL-12',
        address: '127.0.0.1',
        port: 2045,
        family: 'IPv4',
        eplist: ['4@127.0.0.1@2045@u', `4@${LOCAL_IP}@2045@u`],
    },
];

process.on('uncaughtException', function (err) {
    LOG_ERROR('An uncaught error occurred!' + err.message);
    LOG_ERROR(err.stack);
});

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
                addrList: ['0.0.0.0'],
                initPort: this.m_peerinfo.port,
                maxPortOffset: 0,
            }
        });

        let seedList = [];
        for (let nodeInfo of SuperNodeInfoList) {
            let peerInfo = {peerid: nodeInfo.peerid, eplist: nodeInfo.eplist};
            seedList.push(peerInfo);
        }
        p2p.joinDHT(seedList, false);
        this.m_dht = p2p.dht;
        /*
        let sock = dgram.createSocket({type:'udp4', reuseAddr: true});

        sock.on('listening', () => {
            var address = sock.address();
            LOG_INFO(`PEER(${this.m_peerinfo.peerid}) Server listening on ${address.address} : ${address.port}`);
        });
    
        sock.on('message', (message, remote) => {
            // console.log(`PEER(${this.m_peerinfo.peerid}) got package: ${remote.address}: ${remote.port}  - ${message}`);
            let ret = this.m_dht.process(sock, message, remote);
        });
    
        console.log(`PEER(${this.m_peerinfo.peerid}) bind ${this.m_peerinfo.port}`);
        sock.bind(this.m_peerinfo.port);
        
        this.m_dht = new DHT(sock, {peerid: this.m_peerinfo.peerid, eplist: this.m_peerinfo.eplist});
        
        this.m_dht.start();
        console.log(`PEER(${this.m_peerinfo.peerid}) start.`);
        
        // active super node
        for (let nodeInfo of SuperNodeInfoList) {
            let peerInfo = {peerid: nodeInfo.peerid, eplist: nodeInfo.eplist, family: 'IPv4'};
            this.m_dht.activePeer(peerInfo, peerInfo);
        }
        */
    }

    joinSN() {//'XXXXXXXX', 
        let snDHT = this.m_dht.prepareServiceDHT([SN_SERVICE_ID]);
        let snDHTPeer = new DHTPeer(this.m_peerinfo);
        snDHTPeer.m_dht = snDHT;
        snDHT.signinServer();
        return snDHTPeer;
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

    deleteValue(tableName, keyName) {
        this.m_dht.deleteValue(tableName, keyName);
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
        this.m_broadcastEventListener = (event, params, source) => {
            console.log(`PEER<${this.m_peerinfo.peerid}>: Event arrive(name:${eventName}, params: ${params}, source:${JSON.stringify(source)})`);
        }
        this.m_dht.attachBroadcastEventListener(eventName, this.m_broadcastEventListener);
    }

    detachBroadcastEvent(eventName) {
        this.m_dht.detachBroadcastEventListener(eventName, this.m_broadcastEventListener);
        console.log(`PEER<${this.m_peerinfo.peerid}>: detachBroadcastEventListener(${eventName})`);
    }
}

async function main() {
    let superNodeList = [];
    for (let nodeInfo of SuperNodeInfoList) {
        nodeInfo.peerid = `${nodeInfo.peerid}-${DHTUtil.RandomGenerator.string(8)}`;
    }

    let snServiceNodeList = [];
    for (let nodeInfo of SuperNodeInfoList) {
        let node = new DHTPeer(nodeInfo);
        await node.start();
        superNodeList.push(node);
        node.attachBroadcastEvent(BROADCAST_EVENT_NAME);
        let snDHTNode = node.joinSN();
        snDHTNode.attachBroadcastEvent(SN_BROADCAST_EVENT_NAME);
        snServiceNodeList.push(snDHTNode);
    }

    let normalNodeList = [];
    for (let nodeInfo of NormalNodeInfoList) {
        nodeInfo.peerid = `${nodeInfo.peerid}-${DHTUtil.RandomGenerator.string(16)}`;
        let node = new DHTPeer(nodeInfo);
        await node.start();
        normalNodeList.push(node);
        node.attachBroadcastEvent(BROADCAST_EVENT_NAME);
    }

    if (TEST_ENABLE_CONFIG.SET_GET_VALUE) {
        let client = normalNodeList[0];
        client.saveValue('table_1', 'key_string', 'stringValue');
        client.saveValue('table_1', 'key_number', 22222);
        client.saveValue('table_1', 'key_bool', true);
        client.saveValue('table_1', 'key_array', ['stringValue1', 2222, false]);
        client.saveValue('table_1', 'key_object', {o1: 'stringValue1', o2: 2222, o3: false});
        client.saveValue('table_1', 'key_arrayArray', ['stringValue1', [2222, false]]);
        client.saveValue('table_1', 'key_objectArray', ['stringValue1', {o1:2222, o2:false}]);

        client = normalNodeList[1];
        client.saveValue('table_1', 'key1_string', 'stringValue');
        client.saveValue('table_1', 'key1_number', 22222);
        client.saveValue('table_1', 'key1_bool', true);
        client.saveValue('table_1', 'key1_array', ['stringValue1', 2222, false]);
        client.saveValue('table_1', 'key1_object', {o1: 'stringValue1', o2: 2222, o3: false});
        client.saveValue('table_1', 'key1_arrayArray', ['stringValue1', [2222, false]]);
        client.saveValue('table_1', 'key1_objectArray', ['stringValue1', {o1:2222, o2:false}]);


        client = normalNodeList[1];
        client.saveValue('table_3', 'key1_string', 'stringValue3');
        client.saveValue('table_3', 'key1_number', 3333);
        client.saveValue('table_3', 'key1_bool', false);
        client.saveValue('table_3', 'key1_array', ['stringValue1333', 333, true]);
        client.saveValue('table_3', 'key1_object', {o13: 'stringValue1', o23: 2222, o33: false});
        client.saveValue('table_3', 'key1_arrayArray', ['stringValue13', [3333, false]]);
        client.saveValue('table_3', 'key1_objectArray', ['stringValue13', {o13:2222, o23:false}]);
        //*/
        function startGetValue() {
            // get自己保存的值
            client = normalNodeList[0];
            // 精确查找
    /*        client.getValue('table_1', 'key_string', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_number', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_bool', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_array', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_object', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_arrayArray', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_objectArray', DHTUtil.GetValueFlag.Precise);

            // 相近查找
            client.getValue('table_1', 'key_string', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_number', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_bool', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_array', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_object', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_arrayArray', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_objectArray', DHTUtil.GetValueFlag.KeyHashClose);

            // 查找所有
            client.getValue('table_1', DHTUtil.TOTAL_KEY, DHTUtil.GetValueFlag.KeyHashClose);

            // get 其他peer保存的值
            client = normalNodeList[2];
            // 精确查找
            client.getValue('table_1', 'key_string', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_number', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_bool', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_array', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_object', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_arrayArray', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_objectArray', DHTUtil.GetValueFlag.Precise);
            // 相近查找
            client.getValue('table_1', 'key_string', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_number', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_bool', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_array', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_object', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_arrayArray', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_1', 'key_objectArray', DHTUtil.GetValueFlag.KeyHashClose);*/
            // 查找所有
            client.getValue('table_1', DHTUtil.TOTAL_KEY, DHTUtil.GetValueFlag.KeyHashClose);

    /*        // 查找不存在的table
            client.getValue('table_notexist', 'key_string', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_notexist', 'key_string', DHTUtil.GetValueFlag.KeyHashClose);
            client.getValue('table_notexist', DHTUtil.TOTAL_KEY, DHTUtil.GetValueFlag.KeyHashClose);

            // 查找不存在的key
            client.getValue('table_1', 'key_notexist', DHTUtil.GetValueFlag.Precise);
            client.getValue('table_1', 'key_notexist', DHTUtil.GetValueFlag.KeyHashClose);*/
        }

        setTimeout(() => startGetValue(), 5000);
        //setTimeout(() => normalNodeList[0].deleteValue('table_1', 'key_string'), 10000);
        setTimeout(() => normalNodeList[0].getValue('table_1', DHTUtil.TOTAL_KEY, 0), 30000);
        //setTimeout(() => normalNodeList[0].deleteValue('table_1', DHTUtil.TOTAL_KEY), 40000);
        setTimeout(() => normalNodeList[0].getValue('table_1', DHTUtil.TOTAL_KEY, 0), 60000);
    }

    if (TEST_ENABLE_CONFIG.BROADCAST) {
        setInterval(() => {
            /**/
                normalNodeList[5].emitBroadcastEvent(BROADCAST_EVENT_NAME, 0);
                normalNodeList[5].emitBroadcastEvent(BROADCAST_EVENT_NAME, 100);
                normalNodeList[5].emitBroadcastEvent(BROADCAST_EVENT_NAME, null);
                normalNodeList[5].emitBroadcastEvent(BROADCAST_EVENT_NAME, undefined);
                normalNodeList[5].emitBroadcastEvent(BROADCAST_EVENT_NAME, false);
                normalNodeList[5].emitBroadcastEvent(BROADCAST_EVENT_NAME, {a:'a', b:1});
                normalNodeList[5].emitBroadcastEvent(BROADCAST_EVENT_NAME, [1, 'abc', true]);
                normalNodeList[5].emitBroadcastEvent(BROADCAST_EVENT_NAME, [1, {a:'a', b:1}, true]);
                normalNodeList[5].emitBroadcastEvent(BROADCAST_EVENT_NAME, ['stringValue1', [2222, false]]);
                normalNodeList[5].emitBroadcastEvent(BROADCAST_EVENT_NAME, {a:'a', b:1, arr:[1, 'abc', true]});
                normalNodeList[5].emitBroadcastEvent('not exist event', 'not exist event');
            /**/
            }, 1000);
            
            setTimeout(() => {
                superNodeList.forEach(sn => sn.detachBroadcastEvent('not exist event'));
                normalNodeList.forEach(sn => sn.detachBroadcastEvent('not exist event'));
            }, 300000);
            
            setTimeout(() => {
                superNodeList.forEach(sn => sn.detachBroadcastEvent(BROADCAST_EVENT_NAME));
                normalNodeList.forEach(sn => sn.detachBroadcastEvent(BROADCAST_EVENT_NAME));
            }, 600000);
    }


    if (TEST_ENABLE_CONFIG.SUB_SERVICE) {
        setInterval(() => {
                let emiterNode = snServiceNodeList[1];
            /**/
                emiterNode.emitBroadcastEvent(SN_BROADCAST_EVENT_NAME, 0);
                emiterNode.emitBroadcastEvent(SN_BROADCAST_EVENT_NAME, 100);
                emiterNode.emitBroadcastEvent(SN_BROADCAST_EVENT_NAME, null);
                emiterNode.emitBroadcastEvent(SN_BROADCAST_EVENT_NAME, undefined);
                emiterNode.emitBroadcastEvent(SN_BROADCAST_EVENT_NAME, false);
                emiterNode.emitBroadcastEvent(SN_BROADCAST_EVENT_NAME, {a:'a', b:1});
                emiterNode.emitBroadcastEvent(SN_BROADCAST_EVENT_NAME, [1, 'abc', true]);
                emiterNode.emitBroadcastEvent(SN_BROADCAST_EVENT_NAME, [1, {a:'a', b:1}, true]);
                emiterNode.emitBroadcastEvent(SN_BROADCAST_EVENT_NAME, ['stringValue1', [2222, false]]);
                emiterNode.emitBroadcastEvent(SN_BROADCAST_EVENT_NAME, {a:'a', b:1, arr:[1, 'abc', true]});
                emiterNode.emitBroadcastEvent('not exist event', 'not exist event');
            /**/
            }, 1000);
            
            setTimeout(() => {
                snServiceNodeList.forEach(sn => sn.detachBroadcastEvent('not exist event'));
            }, 30000);
            
            setTimeout(() => {
                snServiceNodeList.forEach(sn => sn.detachBroadcastEvent(SN_BROADCAST_EVENT_NAME));
            }, 60000);
    }
}

main();