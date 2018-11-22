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
const SN = require('../../sn/sn.js');
const P2P = require('../../p2p/p2p');
const DHT = require('../../dht/dht.js');
const DHTUtil = require('../../dht/util.js');
const PackageModule = require('../../bdt/package.js');
const BDTPackage = PackageModule.BDTPackage;

const Base = require('../../base/base.js');
const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

process.on('uncaughtException', function (err) {
    LOG_ERROR('An uncaught error occurred!' + err.message);
    LOG_ERROR(err.stack);
});

async function startSNServer(serverConfigList) {
    let dhtEntry = [];
    serverConfigList.forEach(snInfo => dhtEntry.push({peerid: snInfo.peerid, eplist: snInfo.eplist}));

    for (let config of serverConfigList) {
        let {result, p2p} = await P2P.create({
                peerid: config.peerid,
                tcp: {
                    addrList:[config.address],
                    initPort: config.port,
                    maxPortOffset: 0,
                },
                dhtEntry,
            });

        if (result !== 0) {
            LOG_WARN(`SN service listening failed.`);
            continue;
        }

        if (p2p.startupSNService() != 0) {
            LOG_WARN(`SN service listening failed.`);
            continue;
        }

        console.log(`server<${config.peerid}> start.`);
    }
}


if (require.main === module) {
    startSNServer([/*{
            peerid: 'SIMPLE-SN-1',
            address: '127.0.0.1',
            port: 3034,
            eplist: ['4@127.0.0.1@3034', '4@192.168.100.175@3034'],
        },*/
        {
            peerid: 'SIMPLE-SN-2',
            address: '0.0.0.0',
            port: 3035,
            eplist: ['4@127.0.0.1@3035', '4@192.168.100.175@3035'],
        },
        /*{
            peerid: 'SIMPLE-SN-3',
            address: '0.0.0.0',
            port: 3036,
            eplist: ['4@127.0.0.1@3036', '4@192.168.100.175@3036'],
        },*/]);
}

module.exports.start = startSNServer;