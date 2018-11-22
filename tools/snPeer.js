#!/usr/bin/env node
const {
    BX_SetLogLevel,
    BLOG_LEVEL_WARN,
    BLOG_LEVEL_ERROR,
    BLOG_LEVEL_INFO,
    BLOG_LEVEL_ALL,
    BLOG_LEVEL_OFF,
} = require('../base/base');
const P2P = require('../p2p/p2p');
const path = require('path');
const DHTAPPID = require('../base/dhtappid');
const SERVICEID = require('../base/serviceid');


// 默认参数
const defaultParams = {
    out_host: '106.75.175.167',
    peerid: 'SN_PEER',
    tcpPort: 10000,
    udpPort: 10010,
    dhtEntry: '[]',
    asSeed: false,
    debug: true,
    log_level: 'all',
    log_file_dir: path.resolve('./log'),
    log_file_name: 'bdt'
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
console.log(params)

P2P.debug({
    level: params.log_level,
    file_dir: params.log_file_dir,
    file_name: params.log_file_name,
})

async function start() {
    const OUT_HOST = params.out_host
    const { tcpPort, udpPort, peerid } = params

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
    };

    let {result, p2p} = await P2P.create(snDHTServerConfig);

    // 在发现的所有DHT网络中写入自己的SN信息；
    // 其他节点可以通过该DHT网络中的任何节点做为入口接入DHT，并在该DHT网络中搜索到SN信息
    if (params.asSeed) {
        p2p.on(P2P.EVENT.DHTCreate, dht => {
            if (dht.appid !== DHTAPPID.sn) {
                dht.saveValue(SERVICEID.sn, peerid, dht.localPeer.eplist);
            }
        });
    }

    let dhtEntry = JSON.stringify(params.dhtEntry);
    if (!Array.isArray(dhtEntry)) {
        dhtEntry = [dhtEntry];
    }
    await p2p.joinDHT(dhtEntry, {dhtAppID: DHTAPPID.sn, asDefault: true});
    await p2p.startupSNService({minOnlineTime2JoinDHT: 0});

    // 聚合DHT入口
    p2p.startupSuperDHTEntry({autoJoin: true});

    if (params.asSeed) {
        // 定时更新数据
        setInterval(() => {
            p2p.getAllDHT().forEach(dht => {
                if (dht.appid !== DHTAPPID.sn) {
                    dht.saveValue(SERVICEID.sn, peerid, dht.localPeer.eplist);
                }
            });
        }, 600000);
    }
}

start()
