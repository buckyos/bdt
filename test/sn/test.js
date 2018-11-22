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

const {
    blog, BX_SetLogLevel, BLOG_LEVEL_WARN, BLOG_LEVEL_ERROR,
    BLOG_LEVEL_INFO,
    BLOG_LEVEL_ALL,
    BLOG_LEVEL_OFF,
    BaseLib,
} = require('../../base/base');


const P2P = require('../../p2p/p2p');

global.ll = console.log.bind(console)


BX_SetLogLevel(BLOG_LEVEL_INFO);



const defaultParams = {
    out_host: '106.75.175.167',
    tcpPort: 10000,
    udpPort: 10010,
}


let params = process.argv.slice(2)
      .map(val => val.split('='))
      .filter( val => val.length == 2)
      .reduce((params, val) => {
          const [key, value] = val
          params[key] = value
          return params
      }, {})

params = Object.assign(defaultParams, params)

// console.log(params)


!(async () => {
    const OUT_HOST = params.out_host
    const peerid = `${OUT_HOST}#SN`
    const { tcpPort, udpPort } = params

    // 端口配置
    const snDHTServerConfig = {
        // 使用username和本机的ip 拼接 peerid, 方便在不同的主机上启动测试
        peerid: peerid,
        tcp: {
            addrList: ['0.0.0.0'],
            initPort: tcpPort,
            maxPortOffset: 0,
        },
        udp: {
            addrList: ['0.0.0.0'],
            initPort: udpPort,
            maxPortOffset: 0,
        },
        listenerEPList: [`4@${OUT_HOST}@${tcpPort}@t`, `4@${OUT_HOST}@${udpPort}@u`]
    };

    try {
        let {result, p2p} = await P2P.create(snDHTServerConfig);
        await p2p.joinDHT([]);
        await p2p.startupSNService({minOnlineTime2JoinDHT: 0});
    } catch(e) {
        ll(e)
    }

})()



