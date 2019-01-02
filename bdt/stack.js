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
const EventEmitter = require('events');
const dgram = require('dgram');
const {BDT_ERROR, BDTPackage} = require('./package');
const {PingClient, MultiSNPingClient} = require('./pingclient');
const baseModule = require('../base/base');
const blog = baseModule.blog;
const {EndPoint, SequenceU32, TimeHelper} = require('../base/util');
const assert = require('assert');
const DynamicSocket = require('./dynamic_socket');

class BDTStack extends EventEmitter {
    constructor(peerid, eplist, mixSocket, peerFinder, remoteFilter, options) {
        super();
        this.m_peerid = peerid;
        this.m_peeridHash = BDTPackage.hashPeerid(this.m_peerid);

        this.m_pingClient = null;
        this.m_state = BDTStack.STATE.init;

        // vport->acceptor
        // 用于为acceptor分配vport；
        // 处理[syn, calledReq]
        this.m_vportAcceptorMap = {};
        // vport->连出connect
        // 用于为连出connection分配vport；
        // connection和acceptor处理的包类型不同，vport相同也无所谓；
        // 而且通过acceptor建立的连接本地vport也都是相同的，从理论上也是支持这种连接存在的
        // 处理[synAck]
        this.m_vportConnectionMap = {};
        this.m_lastvport = 0;
        // sessionid->connect
        // 用于分配sessionID，每个连接建立(无论是主动connect还是acceptor)都要分配一个本地唯一的sessionid；
        // 在握手阶段[syn,calledReq,synAck]交换各自的sessionid，握手成功后使用对方的sessionid进行通信，
        // 对方收到包后取出自己的sessionid分派给相应的connection处理；
        // 采用(remotePeerHash,remoteVPort)<->(localPeerHash,localVPort)来唯一确定一条连接，一般来说也是可行的；
        // 但我们的PeerHash只有16位，peer数量多的情况下，很容易发生碰撞；而acceptor接受的被动连接，本地信息都是一样的，
        // 一旦发生碰撞，将无法识别这两条连接；而且就算增加hash位数降低碰撞概率，也不能从理论上避免这种碰撞；
        // 所以这里引入sessionid的概念来从理论上避免这种情况发生；
        // 处理[callResp, synAckAck, data, heartbeat, heartbeatResp, fin]
        this.m_sessionConnectMap = {};
        this.m_lastSessionid = 0;
        
        this.m_remoteCache = {};
        this.m_mixSocket = mixSocket;
        this.m_peerFinder = peerFinder;
        this.m_eplist = eplist;
        if (remoteFilter) {
            this.m_remoteFilter = remoteFilter;
        } else {
            this.m_remoteFilter = {
                isForbidden() {
                    return false;
                }
            };
        }

        this.m_dynamicSocket = null;

        this.m_options = {
            // udp mtu
            udpMTU: 1450,
            // udp mms
            udpMMS: 1388,
            // 默认send buffer size
            defaultSendBufferSize: 1024*1388,
            // 通知drain事件时send buffer空闲空间
            drainFreeBufferSize: 1000*1388,
            // 初始接收窗口大小
            initRecvWindowSize: 20*1388,
            // 打洞包的发送间隔
            tryConnectInterval: 1000,
            // 连接超时
            connectTimeout: 15*1000,
            // 对端可用地址未收到包的最长缓存时间
            remoteCacheTimeout: 60*60*1000,
            // 发送ack的最大延时
            ackTimeoutMax: 100,
            // 发送ack的最小延时
            ackTimeoutMin: 40,
            // 延迟发送ack确认的最大数据量，超过必须立即ack
            ackSize: 5 * 1388,
            // 连续立即ack包数量(quickAck计数器)
            quickAckCount: 16,
            // 检查重发队列的周期
            resendInterval: 100,
            // 最小重发时间
            rtoMin: 200,
            // 最大重发时间
            rtoMax: 120*1000,
            // 初始化 rttvar
            initRTTVar: 300, 
            // nagle得时延
            nagleTimeout: 200,
            // 心跳周期
            heartbeatInterval: 1000,
            // 开始心跳的最长未收到包间隔
            heartbeatBeginTimeout: 5*1000,
            // 判定break的最长未收到包间隔
            breakTimeout: 15*1000,
            // timeWait状态的msl值
            msl: 2*1000,
            // 和sn连接时，发ping的周期
            pingConnectInterval: 500,
            // 和sn连接超时
            pingConnectTimeout: 15*1000,
            // 连上sn以后发ping的周期
            pingInterval: 25*1000,
            // 连上sn以后失去ping相应判定掉线的时间
            pingTimeout: 12*25*1000,
            // ping包可延迟时间
            pingDelay: 500,
            // ping包丢失间隔
            pingLostTimeout: 2000,
            // 初始搜索上线SN时间间隔，SN为空
            searchSNIntervalInit: 10809,
            // 连接过程搜索上线SN时间间隔，SN不为空，但都还没上线
            searchSNIntervalConnecting: 30809,
            // 搜索上线SN时间间隔
            searchSNInterval: 608090,
            // 查找对方SN失败后再次尝试间隔
            tryFindSNInterval: 1000,
            // 连接SN数量限制
            snLimit: 3,

            // 最小动态端口
            minDynamicPort: 40809,
            // 最大动态端口
            maxDynamicPort: 60809,
            // 动态端口死亡周期
            dynamicPortDeadTime: 3600809,
            // 扩展动态端口数量
            dynamicExpand: 3,

            // DEBUG开关
            debug: false,
        };
        if (options) {
            Object.assign(this.m_options, options);
        }
    }

    get state() {
        return this.m_state;
    }

    get mixSocket() {
        return this.m_mixSocket;
    }

    get peerid() {
        return this.m_peerid;
    }

    get peeridHash() {
        return this.m_peeridHash;
    }

    get eplist() {
        if (this.m_pingClient) {
            return this.m_pingClient.localEPList;
        }
        return this.m_eplist;
    }

    get listenEPList() {
        return this.m_eplist;
    }

    get peerFinder() {
        return this.m_peerFinder;
    }

    get options() {
        return this.m_options;
    }

    get pingClient() {
        return this.m_pingClient;
    }

    get remoteFilter() {
        return this.m_remoteFilter;
    }

    //event create
    create(callback=null) {
        if (callback) { 
            this._create().then(
                ()=>{callback(BDT_ERROR.success);}, 
                callback);
        } else {
            return this._create();
        }
    }

    //event close
    close(callback=null) {
        if (callback) {
            this._close().then(callback);
        } else {
            return this._close();
        }
    }

    _close() {
        this.m_state = BDTStack.STATE.closing;
        if (this.m_pingClient) {
            this.m_pingClient.close();
            this.m_pingClient = null;
        }
        return new Promise((resolve)=>{
            let tryFinalClose = ()=>{
                if (Object.keys(this.m_sessionConnectMap).length > 0 || Object.keys(this.m_vportAcceptorMap).length > 0) {
                    return;
                }
                this.m_vportConnectionMap = {};
                if (this.m_dynamicSocket) {
                    this.m_dynamicSocket.destroy();
                    this.m_dynamicSocket = null;
                }
                this.m_state = BDTStack.STATE.closed;
                this.emit(BDTStack.EVENT.close);
                resolve();
            };
            
            for (let [vport, acceptor] of Object.entries(this.m_vportAcceptorMap)) {
                acceptor.once('close', () => tryFinalClose());
                acceptor.close();
            }
            for (let [sessionid, connection] of Object.entries(this.m_sessionConnectMap)) {
                connection.once('close', () => tryFinalClose());
                connection.close(true);
            }
        });
    }

    initSeq() {
        return SequenceU32.random();
    }

    _create() {
        blog.info('[BDT]: begin create stack');
        if (this.m_state !== BDTStack.STATE.init) {
            blog.warn('[BDT]: stack create reject for stack is not in init state');
            return Promise.resolve(BDT_ERROR.invalidState);
        } else {
            this._initVPort();
            this._initSessionid();
            blog.debug(`[BDT]: stack random start vport to ${this.m_lastvport}`);

            this._initDynamicSocket();
            this.m_state = BDTStack.STATE.created;
            setImmediate(() => this.emit(BDTStack.EVENT.create));
            
            this.m_pingClient = new MultiSNPingClient(this);
            this.m_pingClient.on(PingClient.EVENT.online, () => setImmediate(() => this.emit(BDTStack.EVENT.online)));
            this.m_pingClient.on(PingClient.EVENT.offline, () => setImmediate(() => this.emit(BDTStack.EVENT.offline)));
            this.m_pingClient.connect();
            return Promise.resolve(BDT_ERROR.success);
        }
    }

    process(socket, decoder, remote) {
        let header = decoder.header;
        if (header.dest.peeridHash !== this.m_peeridHash) {
            return;
        }
        if (decoder.decodeBody()) {
            return;
        }
        let remoteSender = BDTPackage.createSender(this.m_mixSocket, socket, [EndPoint.toString(remote)]);
        return this._packageProcess(decoder, remoteSender, false);
    }
   
    _initVPort() {
        this.m_lastvport = Math.floor(1025 + 60809 * Math.random(0, 1));
    }
   
    _initSessionid() {
        this.m_lastSessionid = Math.floor(160809 + (0x133A129 - 160809) * Math.random(0, 1));
    }

    _genSessionid(connection) {
        if (!connection) {
            return [BDT_ERROR.invalidArgs, null];
        }

        let last = this.m_lastSessionid;
        do {
            last += 1;
            if (last === 0x80000000) {
                last = 1025;
            }
            if (this.m_lastSessionid === last) {
                // all used !
                return [BDT_ERROR.tooMuchConnection, null];
            }
        } while(this.m_sessionConnectMap[last])
        this.m_sessionConnectMap[last] = connection;
        this.m_lastSessionid = last;
        return [BDT_ERROR.success, last];
    }

    _releaseSessionid(sessionid, connection) {
        if (!sessionid || !connection) {
            return ;
        }
        if (this.m_sessionConnectMap[sessionid] === connection) {
            delete this.m_sessionConnectMap[sessionid];
        }
    }

    _genVPort(vport, connection) {
        if (!connection) {
            return [BDT_ERROR.invalidArgs, vport];
        }

        if (vport) {
            let cur = this.m_vportConnectionMap[vport];
            if (cur) {
                if (cur === connection) {
                    return [BDT_ERROR.success, vport];
                } else {
                    return [BDT_ERROR.conflict, vport];
                }
            } else {
                this.m_vportConnectionMap[vport] = connection;
                return [BDT_ERROR.success, vport];
            }
        } else {
            let last = this.m_lastvport;
            do {
                last += 1;
                if (last === 65536) {
                    last = 1025;
                }
                if (this.m_lastvport === last) {
                    // all used !
                    return [BDT_ERROR.conflict, null];
                }
            } while(this.m_vportConnectionMap[last])
            this.m_vportConnectionMap[last] = connection;
            this.m_lastvport = last;
            return [BDT_ERROR.success, last];
        }
    }

    _releaseVPort(vport, connection) {
        if (!vport || !connection) {
            return ;
        }
        if (this.m_vportConnectionMap[vport] === connection) {
            delete this.m_vportConnectionMap[vport];
        }
    }

    _refAcceptor(vport, acceptor) {
        if (!acceptor) {
            return BDT_ERROR.invalidArgs;
        }
        let vportn = parseInt(vport);
        if (vportn.toString() !== vport.toString()) {
            return BDT_ERROR.invalidArgs;
        }

        vport = vportn;
        if (vport) {
            let cur = this.m_vportAcceptorMap[vport];
            if (cur) {
                if (cur === acceptor) {
                    return BDT_ERROR.success;
                } else {
                    return BDT_ERROR.conflict;
                }
            } else {
                this.m_vportAcceptorMap[vport] = acceptor;
                return BDT_ERROR.success;
            }
        } else {
            return BDT_ERROR.invalidArgs;
        }
    }

    _unrefAcceptor(vport, acceptor) {
        if (!vport || !acceptor) {
            return ;
        }
        if (this.m_vportAcceptorMap[vport] === acceptor) {
            delete this.m_vportAcceptorMap[vport];
        }
    }

    _updateRemoteCache(peeridHash, sender) {
        let cache = this.m_remoteCache;
        cache[peeridHash] = {
            sender: sender,
            time: TimeHelper.uptimeMS()
        };
    }

    _invalidRemoteCache(peeridHash, invalidCache) {
        let cache = this.m_remoteCache[peeridHash];
        if (cache) {
            if (cache.time === invalidCache.time) {
                delete this.m_remoteCache[peeridHash];
            }
        }
        return this._getRemoteCache(peeridHash);
    }   

    _getRemoteCache(peeridHash) {
        let cache = this.m_remoteCache[peeridHash];
        if (!cache) {
            return null;
        }
        let delta = TimeHelper.uptimeMS() - cache.time;
        if (delta > this.m_options.remoteCacheTimeout) {
            delete this.m_remoteCache[peeridHash];
            return null;
        }
        return cache;
    }
    

    _getOptions() {
        const opt = this.m_options;
        return opt;
    }

    _findSN(peerid, fromCache, onStep) {
        return this.m_peerFinder.findSN(peerid, fromCache, onStep);
    }

    _findPeer(peerid) {
        return this.m_peerFinder.findPeer(peerid);
    }

    _peerFinder() {
        return this.m_peerFinder;
    }

    _getDynamicSocket() {
        return this.m_dynamicSocket;
    }

    _initDynamicSocket() {
        let options = {
            minPort: this.m_options.minDynamicPort,
            maxPort: this.m_options.maxDynamicPort,
            portDeadTime: this.m_options.dynamicPortDeadTime,
        };

        let dynamicProcess = (socket, decoder, remoteAddr, localAddr) => {
            let header = decoder.header;
            if (header.dest.peeridHash !== this.m_peeridHash) {
                return;
            }
            let remoteSender = BDTPackage.createSender(this.m_dynamicSocket, socket, [EndPoint.toString(remoteAddr)]);
            return this._packageProcess(decoder, remoteSender, true);
        }
        this.m_dynamicSocket = new DynamicSocket(dynamicProcess, options);
    }

    _packageProcess(decoder, remoteSender, isDynamic) {
        let header = decoder.header;
        let remote = EndPoint.toAddress(remoteSender.remoteEPList[0]);
        if (header.cmdType === BDTPackage.CMD_TYPE.pingResp) {
            blog.debug(`[BDT]: stack recv ${BDTPackage.CMD_TYPE.toString(header.cmdType)} package from sn`);
            if (this.m_state === BDTStack.STATE.created) {
                this.m_pingClient._onPackage(decoder, remoteSender);
            }
        } else if (header.cmdType > BDTPackage.CMD_TYPE.pingResp) {
            blog.debug(`[BDT]: stack recv ${BDTPackage.CMD_TYPE.toString(header.cmdType)} package from ${header.src.peeridHash}:${header.src.vport} ${remoteSender.remoteEPList[0]} to ${header.dest.vport}, sessionid:${header.sessionid}, seq:${header.seq}, ackseq:${header.ackSeq}, flags:${header.flags}`);
            if (header.cmdType >= BDTPackage.CMD_TYPE.syn) {
                this._updateRemoteCache(header.src.peeridHash, remoteSender);
            }

            let entry = null;
            if (header.cmdType === BDTPackage.CMD_TYPE.calledReq ||
                header.cmdType === BDTPackage.CMD_TYPE.syn) {
                if (decoder.body && decoder.body.dest === this.peerid) {
                    entry = this.m_vportAcceptorMap[header.dest.vport];
                }
            } else if (header.cmdType === BDTPackage.CMD_TYPE.synAck) {
                if (decoder.body && decoder.body.dest === this.m_peerid) {
                    entry = this.m_vportConnectionMap[header.dest.vport];
                    // 可能用相同端口先后发起了多次连接，用sessionid区分旧连接的过时包
                    if (entry && decoder.body.sessionid) {
                        if (parseInt(decoder.body.sessionid) !== entry.local.sessionid) {
                            entry = null;
                        } else {
                            assert(entry.remote.peerid === decoder.body.src && entry.remote.vport === header.src.vport);
                        }
                    }
                }
            } else {
                entry = this.m_sessionConnectMap[header.sessionid];
            }
            if (entry) {
                entry._onPackage(decoder, remoteSender, isDynamic);
            }
        }
    }

    _filterInvalidAddress(peer) {
        if (peer && peer.eplist) {
            peer.eplist = peer.eplist.filter(ep => !this.m_remoteFilter.isForbidden(ep, peer.peerid));
        }
        return peer;
    }
}


BDTStack.STATE = {
    init: 0,
    created: 1,
    pinging: 2,
    online: 3,
    closing: 10,
    closed: 11,
};

BDTStack.EVENT = {
    create: 'create',
    online: 'online',
    offline: 'offline',
    close: 'close',
    error: 'error'
};

module.exports = BDTStack;