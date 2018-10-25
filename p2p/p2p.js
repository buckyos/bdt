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

const EventEmitter = require('events');
const path = require('path');
const dgram = require('dgram');
const assert = require('assert');
const baseModule = require('../base/base');
const { EndPoint } = require('../base/util')
const blog = baseModule.blog;

const BDT = require('../bdt/bdt.js');
const DHT = require('../dht/dht.js');
const SN = require('../sn/sn.js');
const SNDHT = require('../sn/sn_dht.js');
const PeerFinder = require('./peer_finder.js');
const MixSocket = require('./mix_socket.js');

const {
    BX_SetLogLevel,
    BLOG_LEVEL_INFO,
    BLOG_LEVEL_OFF,
} = baseModule

const PACKAGE_HEADER_SIZE = 8;

class P2P extends EventEmitter {
    constructor() {
        super();
        this.m_peerid = null;
        this.m_udp = {
            addrList: [],
            initPort: 0,
            maxPortOffset: 0,
        };
        this.m_tcp = {
            addrList: [],
            initPort: 0,
            maxPortOffset: 0,
        };
        this.m_epList = [];

        this.m_mixSocket = null;
        this.m_socketCreator = null;
        this.m_dht = null;
        this.m_snPeer = null;
        this.m_peerFinder = null;
        this.m_bdtStack = null;
        this.m_snService = null;
        this.m_isClosing = false;
        this.m_listenerEPList = null;
    }
    
    /*
        创建一个P2P对象，构造基础socket通信接口
        params:
            peerid:string peer id   必填
            udp: {
                addrList:string[] local address list
                initPort:number initial udp port    默认：0，随机PORT
                maxPortOffset:number max try bind port offset   默认：0；initPort=0时，忽略该参数
            }
            tcp: {
                addrList:string[] local address list
                initPort:number initial udp port    默认：0，随机PORT
                maxPortOffset:number max try bind port offset   默认：0；initPort=0时，忽略该参数
            }
            listenerEPList: [ep1,ep2,...]   用户指定的监听EP；
                                            NAT环境下，无法通过udp.addrList和tcp.addrList获知本地PEER的公网访问地址；
                                            可以通过这个参数指定本地PEER的公网访问地址；
                                            如果不指定，则会通过主动对其他PEER的访问响应包分析其公网地址
    */
    static create(params, callback = null) {
        let opt = new Promise(resolve => {
            let p2p = new P2P();
            p2p.m_peerid = params.peerid;

            function initServerParams(server, params) {
                if (params && params.addrList && params.addrList.length > 0) {
                    server.addrList = [...params.addrList];
                } else {
                    return;
                }
    
                server.initPort = params.initPort || 0;
                server.maxPortOffset = params.maxPortOffset || 0;
                if (!server.initPort) {
                    server.maxPortOffset = 0;
                }
            }

            initServerParams(p2p.m_udp, params.udp);
            initServerParams(p2p.m_tcp, params.tcp);
            if (p2p.m_udp.addrList.length + p2p.m_tcp.addrList.length === 0) {
                resolve({result: BDT.ERROR.invalidArgs});
                return;
            }

            p2p.m_listenerEPList = params.listenerEPList || [];

            // create socket
            p2p._createSocket().then(ret => {
                p2p.m_socketCreator = null;
                if (ret !== BDT.ERROR.success) {
                    resolve({result: ret});
                    return;
                }

                setImmediate(() => p2p.emit(P2P.EVENT.create));
                resolve({result: BDT.ERROR.success, p2p});
            });
        });

        if (callback) {
            opt.then(({result, p2p}) => callback({result, p2p}));
        } else {
            return opt;
        }
    }

    /* 
        一步创建一个启动了BDT协议栈的P2P对象，snPeer和dhtEntry必须设定其中一个用于穿透的设施；一般情况使用这个接口就好了
        PS: 该接口整合了create/set snPeer()/joinDHT/startupBDTStack流程，用于构造基本的BDT协议栈；
            如果要对这些接口调用的参数进行定制，需要自行整合这些接口，否则部分接口将使用部分默认参数执行
        params:
            peerid:string peer id
            udp: {
                addrList:string[] local address list
                initPort:number initial udp port    默认：0，随机PORT
                maxPortOffset:number max try bind port offset   默认：0；initPort=0时，忽略该参数
            }
            tcp: {
                addrList:string[] local address list
                initPort:number initial udp port    默认：0，随机PORT
                maxPortOffset:number max try bind port offset   默认：0；initPort=0时，忽略该参数
            }
            snPeer: [{  // 调用set snPeer()
                peerid:
                eplist:
            }]
            dhtEntry: [{    // 调用joinDHT(dhtEntry)
                peerid:
                eplist
            }],
            listenerEPList: [ep1,ep2,...]   用户指定的监听EP；
                                            NAT环境下，无法通过udp.addrList和tcp.addrList获知本地PEER的公网访问地址；
                                            可以通过这个参数指定本地PEER的公网访问地址；
                                            如果不指定，则会通过主动对其他PEER的访问响应包分析其公网地址
            options: {...}  等价于startupBDTStack接口的options参数
    */
    static create4BDTStack(params, callback) {
        function _create4BDTStack() {
            return new Promise(resolve => {
                P2P.create({
                    peerid: params.peerid,
                    udp: params.udp,
                    tcp: params.tcp,
                    listenerEPList: params.listenerEPList,
                }).then(({result, p2p}) => {
                    if (result !== BDT.ERROR.success) {
                        resolve({result, p2p});
                        return;
                    }
            
                    if (params.snPeer) {
                        p2p.snPeer = params.snPeer;
                    }
                    if (params.dhtEntry && params.dhtEntry.length > 0) {
                        p2p.joinDHT(params.dhtEntry);
                    }
            
                    p2p.startupBDTStack(params.options).then(result => {
                        resolve({result, p2p, bdtStack: p2p.bdtStack});
                        return;
                    });
                });
            });
        }

        if (!callback) {
            return _create4BDTStack();
        } else {
            _create4BDTStack().then(ret => callback(ret));
        }
    }

    close() {
        if (this.m_socketCreator) {
            this.m_socketCreator.then(ret => {
                this.m_isClosing = true;
                this._tryCloseSocket();
            })
        }
        if (this.m_mixSocket) {
            this.m_isClosing = true;
            this._tryCloseSocket();
        }

        if (this.m_bdtStack) {
            this.m_bdtStack.close();
        }

        if (this.m_snService) {
            this.m_snService.stop();
        }

        if (this.m_dht) {
            this.m_dht.stop();
        }
    }

    get peerid() {
        this.m_peerid;
    }

    get server() {
        return this.m_mixSocket;
    }

    get eplist() {
        let eplist = null;
        if (this.m_listenerEPList) {
            eplist = new Set(this.m_listenerEPList);
            this.m_mixSocket.eplist.forEach(ep => eplist.add(ep));
            eplist = [... eplist];
        } else {
            eplist = this.m_mixSocket.eplist;
        }

        return eplist;
    }

    // 如果要通过dht网络检索peer，要调用joinDHT加入dht网络
    // dhtEntryPeers: [{peerid: xxx, eplist:[ep, ...]}]， ep: 'family-num(4|6)@ip@port@protocol(u|t)'
    // asSeedSNInDHT: 如果启动了SN服务，标识是否要作为种子SN写入DHT网络
    joinDHT(dhtEntryPeers, asSeedSNInDHT, options) {
        let _options = {
            manualActiveLocalPeer: false,
            joinDHTImmediately: false,
        };

        if (options) {
            Object.assign(_options, options);
        }

        if (!this.m_mixSocket) {
            blog.warn('[P2P]: you should create p2p instance with <P2P.create>, and wait the operation finish.');
        }

        if (!this.m_dht) {
            let eplist = this.eplist;

            this.m_dht = new DHT(this.m_mixSocket, {peerid: this.m_peerid, eplist});
            this.m_dht.once(DHT.EVENT.stop, () => {
                this.m_dht = null;
                this._tryCloseSocket();
            });
            this.m_dht.start(_options.manualActiveLocalPeer);

            if (this.m_peerFinder) {
                this.m_peerFinder.dht = this.m_dht;
            }

            if (this.m_snService) {
                this.m_snService.signinDHT(this.m_dht, asSeedSNInDHT, _options.joinDHTImmediately);
            }
        }

        for (let peer of dhtEntryPeers) {
            this.m_dht.activePeer(peer);
        }
    }

    disjoinDHT() {
        if (this.m_dht) {
            this.m_dht.stop();
        }
    }

    // 如果使用固定的SN检索peer，要设置snPeer属性
    // {peerid: xxx, eplist:[4@ip@port]}
    set snPeer(snPeer) {
        this.m_snPeer = snPeer;
        if (this.m_peerFinder) {
            this.m_peerFinder.snPeer = snPeer;
        }
    }

    // 启动bdt协议栈，在此之前须设置snPeer属性或者调用joinDHT加入DHT网络
    // options指定一些影响bdt运行的参数，建议置null采用默认值
    startupBDTStack(options, callback = null) {
        function getError(error = BDT.ERROR.invalidState) {
            if (!callback) {
                return Promise.resolve(error);
            } else {
                callback(error);
                return error
            }
        }

        // check dhtEntry snPeer
        if (!this.m_dht && !this.m_snPeer) {
            blog.warn('[P2P]: you should set one SN peer(by P2P.snPeer) or dht entry peers (by P2P.joinDHT).');
            return getError()
        }

        // check socket
        if ( !this.m_mixSocket ) {
            blog.warn('[P2P]: you should create p2p instance with <P2P.create>, and wait the operation finish.');
            return getError()
        }


        this.m_peerFinder = new PeerFinder(this.m_snPeer, this.m_dht);

        let eplist = this.eplist;

        this.m_bdtStack = BDT.newStack(this.m_peerid, eplist, this.m_mixSocket, this.m_peerFinder, options);
        this.m_bdtStack.once(BDT.Stack.EVENT.create, () => setImmediate(() => this.emit(P2P.EVENT.BDTStackCreate)));
        this.m_bdtStack.once(BDT.Stack.EVENT.close, () => {
            this.m_bdtStack = null;
            this.m_peerFinder.destory();
            this.m_peerFinder = null;
            this._tryCloseSocket();
            setImmediate(() => this.emit(P2P.EVENT.BDTStackClose));
        });

        // BDT认为连接断了，MixSocket就也保留这个值，保证在BDT连接存续期间socket不失效
        this.m_mixSocket.socketIdle = this.m_bdtStack.options.breakTimeout * 2;
        return this.m_bdtStack.create(callback);
    }

    // 启动SN服务
    startupSNService(asSeedSNInDHT, options) {
        if (!this.m_mixSocket) {
            blog.warn('[P2P]: you should create p2p instance with <P2P.create>, and wait the operation finish.');
            return BDT.ERROR.invalidState;
        }

        this.m_snService = new SN(this.m_peerid, this.m_mixSocket, options);
        this.m_snService.start();
        setImmediate(() => this.emit(P2P.EVENT.SNStart));

        if (this.m_dht) {
            this.m_snService.signinDHT(this.m_dht, asSeedSNInDHT, options.joinDHTImmediately);
        }

        this.m_snService.once(SN.EVENT.stop, () => {
            this.m_snService = null;
            this._tryCloseSocket();
            setImmediate(() => this.emit(P2P.EVENT.SNStop));
        });
        return BDT.ERROR.success;
    }

    get dht() {
        return this.m_dht;
    }

    get bdtStack() {
        return this.m_bdtStack;
    }

    get snService() {
        return this.m_snService;
    }

    _createSocket() {
        blog.info('[P2P]: begin create socket');

        // check the socket was already created
        if (this.m_socketCreator || this.m_mixSocket) {
            blog.warn('[P2P]: socket create reject for function<P2P.create> is called repeatly.');
            return Promise.resolve(BDT.ERROR.invalidState);
        }

        // create a mix socket Instance
        this.m_mixSocket = new MixSocket(
            (...args) => this._udpMessage(...args),
            (...args) => this._tcpMessage(...args),
        );


        const listenerOps = [];
        const addOP = (object, protocol) => {
            const { addrList, initPort, maxPortOffset } = object;
            if ( addrList.length == 0 ) { 
                return
            }
            listenerOps.push(this.m_mixSocket.listen(addrList, initPort, maxPortOffset, protocol));
        }

        addOP(this.m_udp, MixSocket.PROTOCOL.udp);
        addOP(this.m_tcp, MixSocket.PROTOCOL.tcp);
        
        this.m_socketCreator = new Promise(resolve => {
            Promise.all(listenerOps).then(
            ()=>{
                if (this.m_mixSocket.eplist.length > 0) {
                    this.m_mixSocket.once(MixSocket.EVENT.close, () => setImmediate(() => this.emit(P2P.EVENT.close)));
                    return resolve(BDT.ERROR.success);
                }
                return resolve(BDT.ERROR.conflict);
            });
        });

        return this.m_socketCreator;

    }

    _udpMessage(socket, buffer, remoteAddr, localAddr) {
        function commonHeader() {
            let header = {
                magic: buffer.readUInt16LE(0),
                version: buffer.readUInt16LE(2),
                cmdType: buffer.readUInt16LE(4),
                totalLength: buffer.readUInt16LE(6),
            };
            return header;
        }

        if (!(buffer && buffer.length)) {
            return [MixSocket.ERROR.dataCannotParsed];
        }
        if (buffer.length < PACKAGE_HEADER_SIZE) {
            return [MixSocket.ERROR.dataCannotParsed];
        }
        let header = commonHeader();
        if (buffer.length < header.totalLength) {
            return [MixSocket.ERROR.dataCannotParsed];
        }

        return this._selectProcess(socket, header, buffer, remoteAddr, localAddr);
    }

    _tcpMessage(socket, bufferArray, remoteAddr, localAddr) {
        function getBuffer(size) {
            if (bufferArray.totalByteLength < size) {
                return null;
            } else if (bufferArray[0].length >= size) {
                return bufferArray[0].slice(0, size);
            } else {
                let gotBuffer = Buffer.concat(bufferArray, size);
                assert(gotBuffer.length === size, `tcp.totalByteLength,remoteEP:${EndPoint.toString(remoteAddr)},mix.version:${MixSocket.version},lastCmds:${JSON.stringify(socket.__trace.lastCmds)}`);
                return gotBuffer;
            }
        }
        function commonHeader() {
            let headerBuffer = getBuffer(PACKAGE_HEADER_SIZE);
            if (!headerBuffer) {
                return null;
            }
            let header = {
                magic: headerBuffer.readUInt16LE(0),
                version: headerBuffer.readUInt16LE(2),
                cmdType: headerBuffer.readUInt16LE(4),
                totalLength: headerBuffer.readUInt16LE(6),
            };
            return header;
        }

        if (!(bufferArray && bufferArray.totalByteLength && bufferArray.totalByteLength >= PACKAGE_HEADER_SIZE)) {
            return [MixSocket.ERROR.success, 0];
        }
        let header = commonHeader();
        if (!header || header.totalLength > bufferArray.totalByteLength) {
            return [MixSocket.ERROR.success, 0];
        }

        let packageBuffer = getBuffer(header.totalLength);
        if (!packageBuffer || packageBuffer.length < header.totalLength) {
            return [MixSocket.ERROR.success, 0];
        }

        /**
         * // 记录最后5个包的概略信息，用于出现错误时追踪
            if (!socket.__trace.lastCmds) {
                socket.__trace.__cmd_flag = 0x20160809;
                socket.__trace.lastCmds = [];
            }
            header.bufferSize = bufferArray.totalByteLength;
            header.bufferCount = bufferArray.length;
            header.bufferLengths = [];
            bufferArray.forEach(buf => header.bufferLengths.push(buf.length));
            socket.__trace.lastCmds.push(header);
            if (socket.__trace.lastCmds.length > 5) {
                socket.__trace.lastCmds.shift();
                delete socket.__trace.__cmd_flag;
            }
         */

        return this._selectProcess(socket, header, packageBuffer, remoteAddr, localAddr);
    }


    // 根据 cmdType 选择对应的process(DHT, BDT, SN), 对收到包的二进制进行处理
    // @return <array> [errcode, length]
    _selectProcess(socket, header, buffer, remoteAddr, localAddr) {
        const { cmdType, totalLength } = header;

        if (BDT.Package.CMD_TYPE.isValid(cmdType)) {
            if ( totalLength < BDT.Package.HEADER_LENGTH ) {
                return [MixSocket.ERROR.dataCannotParsed, 0];
            }
            let decoder = BDT.Package.createDecoder(buffer);
            if (decoder.decodeHeader()) {
                return [MixSocket.ERROR.dataCannotParsed, totalLength];
            }

            if (this.m_snService && this.m_snService.isMyPackage(decoder)) {
                if (this.m_snService.isAllowed(remoteAddr)) {
                    this.m_snService.process(socket, decoder, remoteAddr, localAddr);
                }
            } else if (this.m_bdtStack) {
                this.m_bdtStack.process(socket, decoder, remoteAddr, localAddr);
            }
            return [MixSocket.ERROR.success, totalLength];
        } else if ( DHT.Package.CommandType.isValid(cmdType) ) {
            if (this.m_dht) {
                if ( totalLength < DHT.Package.HEADER_LENGTH) {
                    return [MixSocket.ERROR.dataCannotParsed, 0];
                }
                this.m_dht.process(socket, buffer, remoteAddr, localAddr);
            }
            return [MixSocket.ERROR.success, totalLength];
        }

        return [MixSocket.ERROR.dataCannotParsed, totalLength];
    }

    _tryCloseSocket() {
        if (this.m_isClosing && !this.m_bdtStack && !this.m_dht && !this.m_snService) {
            if (this.m_mixSocket) {
                // close socket
                let socket = this.m_mixSocket;
                this.m_mixSocket = null;
                socket.close();
                socket.__flag_closed = 0x20160809;
            }
            this.m_isClosing = false;
        }
    }
}

P2P.EVENT = {
    create: 'create',
    close: 'close',
    BDTStackCreate: 'BDTStackCreate',
    BDTStackClose: 'BDTStackClose',
    SNStart: 'SNStart',
    SNStop: 'SNStop',
};

function debug(options = {}) {
    let level = null;
    switch(options.level) {
        case 'all':
            level = baseModule.BLOG_LEVEL_ALL;
            break;
        case 'trace':
            level = baseModule.BLOG_LEVEL_TRACE;
            break;
        case 'debug':
            level = baseModule.BLOG_LEVEL_DEBUG;
            break;
        case 'info':
            level = baseModule.BLOG_LEVEL_INFO;
            break;
        case 'warn':
            level = baseModule.BLOG_LEVEL_WARN;
            break;
        case 'error':
            level = baseModule.BLOG_LEVEL_ERROR;
            break;
        case 'check':
            level = baseModule.BLOG_LEVEL_CHECK;
            break;
        case 'fatal':
            level = baseModule.BLOG_LEVEL_FATAL;
            break;
        case 'off':
            level = baseModule.BLOG_LEVEL_OFF;
            break;
        default:
            level = baseModule.BLOG_LEVEL_OFF;
    }

    BX_SetLogLevel(level);

    if (typeof options.file_dir == 'string'  && options.file_dir.length > 0) {
        baseModule.BX_EnableFileLog(path.resolve(options.file_dir), options.file_name, '.log');
        baseModule.blog.enableConsoleTarget(false);
    }
}

/**
 * 启动BDT协议栈基本流程：
 * let {result, p2p} = await P2P.create(params);
 * p2p.joinDHT([dhtPeer1, dhtPeer2, ...]);
 * p2p.snPeer = [{peerid, eplist}, ...]
 * await p2p.startupBDTStack();
 * let bdtStack = p2p.bdtStack; // BDT协议栈
 * 
 * create4BDTStack整合了上述流程，也可以直接调用它创建BDT协议栈：
 * let {result, p2p, bdtStack} = await P2P.create4BDTStack(params);
 * 
 * 启动SN服务基本流程：
 * let {result, p2p} = await create(params);
 * p2p.joinDHT([dhtPeer1, dhtPeer2, ...]);
 * p2p.startupSNService();
 */
module.exports = {
    create: P2P.create,
    create4BDTStack: P2P.create4BDTStack,
    Stack: BDT.Stack,
    Connection: BDT.Connection,
    Acceptor: BDT.Acceptor,
    DHT,
    SN,
    EVENT: P2P.EVENT,
    ERROR: BDT.ERROR,

    // so that caller can close the log of bdt
    debug: debug,
};
