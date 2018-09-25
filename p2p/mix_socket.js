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
const dgram = require('dgram');
const net = require('net');
const {EndPoint, TimeHelper} = require('../base/util.js');
const assert = require('assert');
const baseModule = require('../base/base');
const blog = baseModule.blog;
const PaceSender = require('./pace_sender.js');

// <TODO> 丢包测试
function lostPackage() {
    return false;
    return Math.round(Math.random() * 5) === 0;
}

// 对0.0.0.0地址替换为回环地址
function mapZeroIP(remoteAddr) {
    let result = remoteAddr.address;
    if (!result || EndPoint.isZero(remoteAddr)) {
        result = EndPoint.loopback(remoteAddr.protocol);
    }
    return result;
}

// 多地址混合socket
class MixSocket extends EventEmitter {
    /**
     * udpProcess = (socket, buffer, remoteAddr, localAddr) => {
     *      ...
     *      if (error) {
     *          return [MixSocket.ERROR.dataCannotParsed];
     *      }
     *      let package = decode(buffer);
     *      if (package) {
     *          return [MixSocket.ERROR.success, package.totalLength];
     *      }
     *      return [MixSocket.ERROR.dataCannotParsed];
     * }
     * tcpProcess = (socket, bufferArray, remoteAddr, localAddr) => {
     *      ...
     *      if (error) {
     *          return [MixSocket.ERROR.dataCannotParsed];
     *      }
     *      let package = decode(bufferArray);
     *      return [MixSocket.ERROR.success, package? package.totalLength : 0];
     * }
     */ 
    constructor(udpProcess, tcpProcess, options) {
        super();
        this.EVENT = MixSocket.EVENT;
        this.PROTOCOL = MixSocket.PROTOCOL;
        this.ERROR = MixSocket.ERROR;

        this.m_udpListeners = new Map(); // <ep, {socket}>
        this.m_udpListeners.asyncOps = new Set();
        this.m_udpListeners.process = udpProcess;
        this.m_tcpListeners = new Map(); // <ep, {socket}>
        this.m_tcpListeners.asyncOps = new Set();
        this.m_tcpListeners.process = tcpProcess;

        this.m_routeTable = new Map(); // <toEP, {connections: [{socket, protocol, lastRecvTime, lastSendTime, recvQueue}]}>
        this.m_routeTable.cleanTimer = null;

        this.m_asyncCloseOp = null;

        this.m_pendingConnections = new Map(); // <'fromEP-toEP', {callbacks}>

        this.m_paceSender = new PaceSender();
        this.m_paceSender.start();
        this.m_paceSender.on(PaceSender.EVENT.pop, (pkg, eplist, options) => this._sendImmediate(pkg, eplist, options));

//<<<<<<<<<统计诊断
        this.m_stat = {
            udp: {
                send: {
                    pkgs: 0,
                    bytes: 0,
                },
                recv: {
                    pkgs: 0,
                    bytes: 0,
                },
            },
            tcp: {
                send: {
                    pkgs: 0,
                    bytes: 0,
                },
                recv: {
                    pkgs: 0,
                    bytes: 0,
                },
            }
        };
//>>>>>>>>>>>>>>>>>

        this.m_options = {
            socketIdle: 600000,
            udpPriorMS: 10000, // CACHE中同时UDP和TCP，UDP优先时间
            tcpSendBufferSize: (1388 * 2),
        };
        if (options) {
            Object.assign(this.m_options, options);
        }
    }

    get socketIdle() {
        return this.m_options.socketIdle;
    }

    set socketIdle(newValue) {
        this.m_options.socketIdle = newValue;
    }

    listen(ipList, port = 0, maxPortOffset = 0, protocol = MixSocket.PROTOCOL.udp) {
        // return a error infomation
        let getArgsError = (__ipList = ipList, error = MixSocket.ERROR.invalidArgs) => {
            let failedAddressList = __ipList.map( ip => {
                return {ip, error};
            });

            return Promise.resolve({error, failedAddressList, succAddressList: []});
        }

        if (this.m_asyncCloseOp) {
            return getArgsError([], MixSocket.ERROR.statusConflict);
        }

        // check ipList is empty
        if (!ipList || !ipList.length) {
            return getArgsError([]);
        }

        // check the local port is in range
        if (port < 0 || port > 65535 || maxPortOffset < 0) {
            return getArgsError();
        }

        let bindIP = null;
        let listeners = null;
        let endPort = port;
        if (port) {
            endPort = Math.min(port + maxPortOffset, 65535);
        }

        if ( EndPoint.isUDP(protocol) ) {
            bindIP = ip => this._bindUDP(ip, port, endPort);
            listeners = this.m_udpListeners;
        } else if ( EndPoint.isTCP(protocol) ) {
            bindIP = ip => this._bindTCP(ip, port, endPort);
            listeners = this.m_tcpListeners;
        } else {
            return getArgsError();
        }

        let failedAddressList = [];
        let succAddressList = [];

        // 将ip{array<string>}列表转换成 listeners{array<Promise>}列表
        let bindOps = ipList.filter(ip => {
            let isIpOk = net.isIP(ip)
            // 不符合格式的ip记录在失败列表
            if ( !isIpOk ) {
                failedAddressList.push({ip, error: MixSocket.ERROR.invalidArgs})
            }
            return isIpOk
        }).map(ip => {
            return new Promise(resolve => {
                bindIP(ip).then(({error, socket, localAddr}) => {
                    if (error) {
                        // 绑定失败的ip记录在失败列表
                        failedAddressList.push({ip, error});
                    } else {
                        succAddressList.push({ip, port: localAddr.port});
                        listeners.set(EndPoint.toString(localAddr), {socket});
                    }
                    resolve();
                });
            })
        })

        let allOp = new Promise(resolve => {
            Promise.all(bindOps).then(() => {
                let error = MixSocket.ERROR.success;
                if (succAddressList.length === 0) {
                    error = MixSocket.ERROR.addressConflict;
                } else {
                    if (!this.m_routeTable.cleanTimer) {
                        this.m_routeTable.cleanTimer = setInterval(() => this._cleanRouteTable(), Math.min(5000, this.m_options.socketIdle));
                    }
                }
                listeners.asyncOps.delete(allOp);
                resolve({error, failedAddressList, succAddressList});
            });
        });
        listeners.asyncOps.add(allOp);
        return allOp;
    }
        
    get eplist() {
        return [...this.m_udpListeners.keys(), ...this.m_tcpListeners.keys()];
    }

    close() {
        // 处理listen返回前的close操作
        if (this.m_asyncCloseOp) {
            return this.m_asyncCloseOp;
        }

        this.m_paceSender.stop();

        this.m_asyncCloseOp = new Promise(resolve => {
            let closeAllSockets = () => {
                let socketCount = 0;
                this.m_udpListeners.forEach(listener => {
                    socketCount++;
                    listener.socket.close();
                });
                this.m_tcpListeners.forEach(listener => {
                    socketCount++;
                    listener.socket.close();
                });
                this.m_routeTable.forEach(routeTable => 
                    routeTable.connections.forEach(connection => {
                        if (connection.protocol === MixSocket.PROTOCOL.tcp && !connection.socket.destroyed) {
                            socketCount++;
                            connection.socket.destroy();
                        }
                    })
                );

                if (socketCount > 0) {
                    this.once(MixSocket.EVENT.close, () => {
                        this.m_asyncCloseOp = null;
                        resolve();
                    });
                } else {
                    resolve();
                }

                if (this.m_routeTable.cleanTimer) {
                    clearInterval(this.m_routeTable.cleanTimer);
                    this.m_routeTable.cleanTimer = null;
                }
            }
            if (this.m_udpListeners.asyncOps.size > 0 || this.m_tcpListeners.asyncOps.size > 0) {
                Promise.all([...this.m_udpListeners.asyncOps, ...this.m_tcpListeners.asyncOps]).then(() => closeAllSockets());
            } else {
                closeAllSockets();
            }
        });
        return this.m_asyncCloseOp;
    }

    // eplist: buffer要发送到的目标peer的ep集合，它们属于同一个peer；
    //          如果没有跟该peer通信过，会尝试用所有监听地址对eplist所有ep发送；
    //          否则，只向其中最近一次跟本地peer通信的ep发送，继续使用当时的socket；
    // options.ignoreCache: =true时，不管以前通过哪个socket/ep收到过包，都尝试从所有监听地址对eplist中所有ep发包
    // options.socket: 指定socket的情况下，使用指定socket发送；
    //          否则，采用最后一次从eplist收到包的socket；
    // options.onPreSend: (pkg, remoteAddr, socket, protocol) {let sendBuffer = encode(pkg); return sendBuffer;}
    //          返回false标识取消发送
    // options.onPostSend: (pkg, remoteAddr, socket, protocol) {...} 发送后通知一下
    // options.dropBusyTCP: 标识在忙碌TCP连接上是否丢弃，一般会重试的消息可以丢弃，讲求顺序和可靠性的消息不可以丢弃
    // options.timeout: 发包的最大延迟时间，默认是0
    // 说明: 1.通常只要用send(buffer, eplist)就好；
    //          如果没有响应，可能缓存信息已经失效，重发时用send(buffer, eplist, true)
    //          如果是创建了一个TCP连接，要在上面顺序性发送数据就用send(buffer, eplist, false, socket)
    //      2.send函数不保证数据的可达性，需要调用方通过响应包确定对方是否收到
    send(pkg, eplist, options) {
        // check the params type and length
        if (!pkg || !Array.isArray(eplist) || !eplist.length) {
            return MixSocket.ERROR.invalidArgs;
        }

        this.m_paceSender.push(pkg, eplist, options);
        return MixSocket.ERROR.success;
    }

    _sendImmediate(pkg, eplist, options) {
        options = options || {};
        let ignoreCache = options.ignoreCache;
        let socket = options.socket;
        let onPreSend = options.onPreSend;
        let onPostSend = options.onPostSend;
        let dropBusyTCP = options.dropBusyTCP;

        eplist = new Set(eplist);
        
        let localHostList = [];
        let lanEPList = [];
        let internatEPList = [];

        // 一个peer可能有本地地址/局域网地址/公网地址；
        // 本地地址可用于本机的多个peer互连，局域网地址可用于同局域网内peer互连；
        // 但它们都不是唯一的，可能多个peer具有相同的本地/局域网地址；
        // 大多数节点都是跨子网分布，只有公网地址是可达的；
        // 本地/局域网地址仅仅是提速和辅助作用，毕竟本地连接消耗小；
        // 所以这里要对几种地址分开发包，保障能从公网地址尝试发送；
        // 否则，万一不同局域网内多个peer的局域网地址相同，连接成一个后，
        // 因为cache的原因，其他peer也复用该连接，导致其他peer无法到达；
        eplist.forEach(ep => {
            if (EndPoint.isLoopback(ep) || EndPoint.isZero(ep)) {
                localHostList.push(ep);
            } else if (EndPoint.isNAT(ep)) {
                lanEPList.push(ep);
            } else {
                internatEPList.push(ep);
            }
        });

        const send = (list) => {
            if ( list.length > 0 ) {
                this._sendImmediate2(pkg, list, ignoreCache, socket, onPreSend, onPostSend, dropBusyTCP);
            }
        }

        send(localHostList);
        send(lanEPList);
        send(internatEPList);
    }

    _sendImmediate2(pkg, eplist, ignoreCache = false, socket = null, onPreSend = null, onPostSend = null, dropBusyTCP = true) {
        if (ignoreCache) {
            socket = null;
        }

        let sendCount = 0;
        let sendTo = (pkg, remoteAddr, sendingSocket, protocol) => {
            if (!remoteAddr) {
                return;
            }

            let getSendingBuffer = () => {
                let buffer = pkg;
                if (onPreSend) {
                    buffer = onPreSend(pkg, remoteAddr, sendingSocket, protocol);
                    return buffer;
                }
                if ((typeof buffer) != 'string' && !(buffer instanceof Buffer)) {
                    buffer = JSON.stringify(buffer);
                }
                return buffer;
            }

            if (protocol === MixSocket.PROTOCOL.udp) {
                if (lostPackage()) {
                    return;
                }
                let {connection} = this._updateRouteTable(sendingSocket, remoteAddr, protocol, false);
                let buffer = getSendingBuffer();
                if (!buffer) {
                    return;
                }
                if (!EndPoint.isNAT(remoteAddr)) {
                    this.m_stat.udp.send.pkgs++;
                    this.m_stat.udp.send.bytes += buffer.length;
                }
                // blog.info(`send udp: (${buffer.length}|${sendCount}),total:(${this.m_stat.udp.send.bytes}|${this.m_stat.udp.send.pkgs})`);
                sendCount++;
                // 用系统默认地址（localhost）替换'0.0.0.0'
                sendingSocket.send(buffer, remoteAddr.port, mapZeroIP(remoteAddr),
                    err => {
                        if (err) {
                            blog.error(`Send package error: ${err.message}`);
                        } else if (onPostSend) {
                            onPostSend(pkg, remoteAddr, sendingSocket, protocol);
                        }
                    });
            } else if (protocol === MixSocket.PROTOCOL.tcp) {
                if (sendingSocket.destroyed) {
                    return;
                }
                let {connection} = this._updateRouteTable(sendingSocket, remoteAddr, protocol, false);
                // console.log(`send:drop:${dropBusyTCP},size:${buffer.length}, sendingLength:${connection.sendingLength}`);
                if (!dropBusyTCP || connection.sendingLength < this.m_options.tcpSendBufferSize) {
                    let buffer = getSendingBuffer();
                    if (!buffer) {
                        return;
                    }

                    if (!EndPoint.isNAT(remoteAddr)) {
                        this.m_stat.tcp.send.pkgs++;
                        this.m_stat.tcp.send.bytes += buffer.length;
                    }
                    // blog.info(`send tcp: (${buffer.length}|${sendCount}),total:(${this.m_stat.tcp.send.bytes}|${this.m_stat.tcp.send.pkgs})`);
                    sendCount++;
    
                    connection.sendQueue.push(buffer);
                    connection.sendingLength += buffer.length;
                    // 所有包都一起send的，无所谓pkg<=>callback的映射
                    if (onPostSend) {
                        connection.sendCallbacks.push(onPostSend);
                    }
                    
                    let sendLeft = () => {
                        connection.sendWaiting = false;
                        if (connection.sendQueue.length === 0 || sendingSocket.destroyed) {
                            return;
                        }
                        
                        let sendingBuffer = null;
                        if (connection.sendQueue.length === 1) {
                            sendingBuffer = connection.sendQueue[0].slice(0);
                        } else {
                            sendingBuffer = Buffer.concat(connection.sendQueue);
                        }

                        assert(sendingBuffer.length === connection.sendingLength);
                        connection.sendingLength = 0;
                        connection.sendQueue.splice(0, connection.sendQueue.length);
                        let sendCallbacks = connection.sendCallbacks;
                        connection.sendCallbacks = [];
    
                        sendingBuffer.__trace = {
                            seq: sendingSocket.__trace.nextSeq
                        };
                        sendingSocket.__trace.pendingSeqList.push(sendingBuffer.__trace.seq);
                        sendingSocket.__trace.nextSeq++;
    
                        connection.sendWaiting = !sendingSocket.write(sendingBuffer, () => {
                            sendCallbacks.forEach(cb => onPostSend(pkg, remoteAddr, sendingSocket, protocol));
                            let headSeq = sendingSocket.__trace.pendingSeqList.shift();
                            assert(headSeq === sendingBuffer.__trace.seq || sendingSocket.destroyed, `tcp seq error(${MixSocket.version}),seq:${sendingBuffer.__trace.seq},headSeq:${headSeq},connecting:${sendingSocket.connecting},destroyed:${sendingSocket.destroyed}`);
                        });
                        
                        if (connection.sendWaiting) {
                            sendingSocket.once('drain', sendLeft);
                        }
                    }
    
                    if (!connection.sendWaiting) {
                        sendLeft();
                    }
                }
            }
        }

        let now = TimeHelper.uptimeMS();
        // UDP接收时间更新一小段，当TCP和UDP接收时间接近时，能优先使用UDP
        let udpPriorRecvTime = connection => {
            let lastRecvTime = connection.lastRecvTime;
            if (lastRecvTime && connection.protocol === EndPoint.PROTOCOL.udp) {
                lastRecvTime += this.m_options.udpPriorMS;
            }
            return lastRecvTime;
        }

        let findLastRecvConnection = (remoteEP, protocol, socket) => {
            let lastConnection = {lastRecvTime: (0 - this.m_options.socketIdle)};
            let routeTable = this.m_routeTable.get(remoteEP);
            if (routeTable) {
                for (let connection of routeTable.connections) {
                    if ((!protocol || connection.protocol === protocol) &&
                        (!socket || connection.socket === socket) &&
                        udpPriorRecvTime(connection) > udpPriorRecvTime(lastConnection)) {
                        lastConnection = connection;
                    }
                }
            }
            return lastConnection;
        }

        if (socket) {
            let lastRecvConnection = {
                remoteEP : null,
                connection: {lastRecvTime: (0 - this.m_options.socketIdle)},
            };

            // 用指定socket，在cache中找到相应的缓存信息
            for (let remoteEP of eplist) {
                let connection = findLastRecvConnection(remoteEP, null, socket);
                if (udpPriorRecvTime(connection) > udpPriorRecvTime(lastRecvConnection.connection)) {
                    lastRecvConnection.remoteEP = remoteEP;
                    lastRecvConnection.connection = connection;
                }
            }

            // 在缓存里没找到合适的连接，或者只找到了无效的UDP连接，就在UDP监听soket里搜索匹配的socket；
            // 对TCP来说，一个socket只有一个连接，按socket发送数据，只能从缓存里搜索，不管有没有超时，都必须使用
            if (lastRecvConnection.connection.lastRecvTime < 0 ||
                (lastRecvConnection.connection.protocol === EndPoint.PROTOCOL.udp && !this._isConnectionValid(lastRecvConnection.connection, now))) {

                    for ( let [localEP, udpListener] of this.m_udpListeners ) {
                        let localAddr = EndPoint.toAddress(localEP);
                        assert(localAddr);
                        if (udpListener.socket === socket) {
                            for (let remoteEP of eplist) {
                                let remoteAddr = EndPoint.toAddress(remoteEP);
                                if (remoteAddr &&
                                    (!remoteAddr.family || !localAddr.family || remoteAddr.family === localAddr.family) &&
                                    remoteAddr.protocol === MixSocket.PROTOCOL.udp) {
                                    sendTo(pkg, remoteAddr, socket, remoteAddr.protocol);
                                }
                            }
                            break;
                        }
                    }
            }

            // 如果从缓存里找到连接，在其上面发送一次
            if (sendCount === 0 && lastRecvConnection.connection.lastRecvTime >= 0) {
                sendTo(pkg,
                    EndPoint.toAddress(lastRecvConnection.remoteEP),
                    socket,
                    lastRecvConnection.connection.protocol);
            }
            return sendCount > 0? MixSocket.ERROR.success : MixSocket.ERROR.socketNotFound;
        }

        if (!ignoreCache) {
            // 利用缓存连接发送
            let lastRecvConnection = {
                remoteEP : null,
                connection: {lastRecvTime: (0 - this.m_options.socketIdle)},
            };
            for (let remoteEP of eplist) {
                let connection = findLastRecvConnection(remoteEP);
                if (udpPriorRecvTime(connection) > udpPriorRecvTime(lastRecvConnection.connection)) {
                    lastRecvConnection.remoteEP = remoteEP;
                    lastRecvConnection.connection = connection;
                }
            }
            // 缓存中找到相关有效信息
            if (this._isConnectionValid(lastRecvConnection.connection, now)) {
                return sendTo(pkg,
                    EndPoint.toAddress(lastRecvConnection.remoteEP),
                    lastRecvConnection.connection.socket,
                    lastRecvConnection.connection.protocol);
            } else {
                this._cleanRouteTable();
            }
        }

        // send package to all address in eplist
        // localEPList * eplist发包
        eplist.forEach(remoteEP => {
            // @var object
            let remoteAddr = EndPoint.toAddress(remoteEP);
            if (!remoteAddr) {
                return;
            }

            // check if udp address
            if (EndPoint.isUDP(remoteAddr)) {
                this.m_udpListeners.forEach((listener, localEP) => {
                    let localAddr = EndPoint.toAddress(localEP);
                    assert(localAddr);
                    if (remoteAddr.family && localAddr.family && localAddr.family !== remoteAddr.family) {
                        return;
                    }
                    sendTo(pkg, remoteAddr, listener.socket, MixSocket.PROTOCOL.udp);
                });
            }

            // check if tcp address
            if (EndPoint.isTCP(remoteAddr)) {
                let cacheConnection = findLastRecvConnection(remoteEP, MixSocket.PROTOCOL.tcp);
                if (!this._isConnectionTimeout(cacheConnection, now)) {
                    sendTo(pkg, remoteAddr, cacheConnection.socket, cacheConnection.protocol);
                    return;
                } else {
                    this._cleanRouteTable();
                }

                let isConnectSucc = false;
                let onConnect = socket => {
                    if (socket) {
                        // 连通一次就行了
                        if (isConnectSucc) {
                            socket.destroy();
                            return;
                        }
                        isConnectSucc = true;
                        sendTo(pkg, remoteAddr, socket, MixSocket.PROTOCOL.tcp);
                    }
                }

                this.m_tcpListeners.forEach((listener, localEP) => {
                    let localAddr = EndPoint.toAddress(localEP);
                    if (remoteAddr.family && localAddr.family && localAddr.family !== remoteAddr.family) {
                        return;
                    }

                    // createSocket -> connect -> send
                    this._connectTCP(remoteAddr, localAddr, true).then(({error, socket}) => onConnect(socket));
                });

                // 尝试用本地随机地址连接一次
                this._connectTCP(remoteAddr, null, false).then(({error, socket}) => onConnect(socket));
            }
        });
        return MixSocket.ERROR.success;
    }

    _udpMessage(connection, msg, remoteAddr, localAddr) {
        if (lostPackage()) {
            return;
        }

        if (msg && msg.length && this.m_udpListeners.process) {
            remoteAddr.protocol = MixSocket.PROTOCOL.udp;
            if (!EndPoint.isNAT(remoteAddr)) {
                this.m_stat.udp.recv.pkgs++;
                this.m_stat.udp.recv.bytes += msg.length;
            }
            // blog.info(`recv udp: (${msg.length}), total: (${this.m_stat.udp.recv.bytes}|${this.m_stat.udp.recv.pkgs})`);

            let offset = 0;
            while (offset < msg.length) {
                let [errorCode, count] = this.m_udpListeners.process(connection.socket, msg.slice(offset), remoteAddr, localAddr);
                if (errorCode === MixSocket.ERROR.success) {
                    offset += count;
                } else {
                    return;
                }
            }
        }
    }

    // handle the tcp package buffer after accept a tcp package
    _tcpMessage(connection, msg, remoteAddr, localAddr) {
        if (msg && msg.length && this.m_tcpListeners.process && connection.socket && !connection.socket.destroyed) {
            remoteAddr.protocol = MixSocket.PROTOCOL.tcp;

            if (!EndPoint.isNAT(remoteAddr)) {
                this.m_stat.tcp.recv.pkgs++;
                this.m_stat.tcp.recv.bytes += msg.length;
            }
            // blog.info(`recv tcp: (${msg.length}), total: (${this.m_stat.tcp.recv.bytes}|${this.m_stat.tcp.recv.pkgs})`);
            
            let recvQueue = connection.recvQueue;
            recvQueue.push(msg);
            recvQueue.totalByteLength += msg.length;
            while (recvQueue.length) {
                // called the p2p/p2p.js _tcpMessage
                let [errorCode, count] = this.m_tcpListeners.process(connection.socket, recvQueue, remoteAddr, localAddr);
                if (errorCode !== MixSocket.ERROR.success) {
                    recvQueue.splice(0, recvQueue.length);
                    recvQueue.totalByteLength = 0;
                    let error = MixSocket.ERROR.dataCannotParsed;
                    connection.socket.destroy(error);
                    return;
                } else if (count === 0) {
                    return;
                } else {
                    recvQueue.totalByteLength -= count;
                    while (count) {
                        if (count >= recvQueue[0].length) {
                            count -= recvQueue.shift().length;
                        } else {
                            recvQueue[0] = recvQueue[0].slice(count);
                            count = 0;
                        }
                    }
                }
            }
        }
    }

    _bindUDP(ip, port, endPort) {
        return new Promise(resolve => {
            let bindSucc = false;

            // to avoid cross-border error
            // transform 'port' to int , no matter it is a string or number
            let curPort = parseInt(port);

            let tryBindNextPort = () => {
                let localAddr = {
                    family: net.isIPv4(ip)? EndPoint.FAMILY.IPv4: EndPoint.FAMILY.IPv6,
                    address: ip,
                    port: curPort,
                    protocol: MixSocket.PROTOCOL.udp,
                }
                const socket = dgram.createSocket({type: localAddr.family === EndPoint.FAMILY.IPv4 ? 'udp4': 'udp6'/*, reuseAddr: true*/});
                socket.once('listening', () => {
                    socket.on('message', (msg, rinfo) => {
                        let remoteAddr = {
                            address: rinfo.address,
                            port: rinfo.port,
                            family: rinfo.family,
                        };
                        remoteAddr.protocol = MixSocket.PROTOCOL.udp;
                        let {connection} = this._updateRouteTable(socket, remoteAddr, MixSocket.PROTOCOL.udp);
                        setImmediate(() => this._udpMessage(connection, msg.slice(0, rinfo.size), remoteAddr, localAddr));
                    });
                    
                    blog.info(`[mixsock]: socket bind udp ${ip}:${curPort} success`);
                    bindSucc = true;
                    localAddr = socket.address();
                    localAddr.protocol = MixSocket.PROTOCOL.udp;
                    resolve({error: MixSocket.ERROR.success, socket, localAddr});
                });

                socket.once('close', () => setImmediate(() => 
                    this._onSocketClosed(socket, localAddr, null, MixSocket.PROTOCOL.udp)
                ));

                socket.on('error', error => {
                    blog.warn(`[mixsock]: socket bind udp ${ip}:${curPort} failed, error:${error}.`);
                    setImmediate(() => this._onSocketError(error, socket, localAddr, null, MixSocket.PROTOCOL.udp));
                    socket.close();
                    if (bindSucc || this.m_asyncCloseOp) {
                        return;
                    }
                    if (curPort < endPort) {
                        curPort += 1;
                        tryBindNextPort();
                    } else {
                        resolve({error: MixSocket.ERROR.addressConflict});
                    }
                });
                socket.bind(curPort, ip);
            }
            tryBindNextPort();
        });
    }

    _bindTCP(ip, port, endPort) {
        return new Promise(resolve => {
            let bindSucc = false;

            // to avoid cross-border error
            // transform 'port' to int , no matter it is a string or number
            let curPort = parseInt(port);
            
            let tryBindNextPort = () => {
                let localAddr = {
                    family: net.isIPv4(ip)? EndPoint.FAMILY.IPv4 : EndPoint.FAMILY.IPv6,
                    address: ip,
                    port: curPort,
                    protocol: MixSocket.PROTOCOL.tcp,
                }

                const server = net.createServer(clientSocket => {
                    // 文档上说：如果socket被销毁，remoteAddress为undefined；
                    // 这里remoteAddress可能为undefined，但是destroyed标志为false,readable和writable都为true；
                    // 具体原因不明，先简单防御一下
                    //*
                    if (!clientSocket.remoteAddress) {
                        clientSocket.destroy();
                        return;
                    }//*/
                    let remoteAddr = {
                        family: clientSocket.remoteFamily,
                        address: clientSocket.remoteAddress,
                        port: clientSocket.remotePort,
                        protocol: MixSocket.PROTOCOL.tcp,
                    }
                    assert(remoteAddr.address, `remoteAddr:${JSON.stringify(remoteAddr)},destroyed:${clientSocket.destroyed},readable:${clientSocket.readable},writable:${clientSocket.writable}`);
                    this._onTCPClientCreated(clientSocket, localAddr, remoteAddr, true, true);
                });
                server.once('listening', () => {
                    blog.info(`[mixsock]: socket bind tcp ${ip}:${curPort} success`);
                    bindSucc = true;
                    localAddr = server.address();
                    localAddr.protocol = MixSocket.PROTOCOL.tcp;
                    resolve({error: MixSocket.ERROR.success, socket: server, localAddr});
                });

                server.once('close', () => setImmediate(() => 
                    this._onSocketClosed(server, localAddr, null, MixSocket.PROTOCOL.tcp)
                ));

                server.on('error', error => {
                    blog.warn(`[mixsock]: socket bind tcp ${ip}:${curPort} failed, error:${error}.`);
                    setImmediate(() => this._onSocketError(error, server, localAddr, null, MixSocket.PROTOCOL.tcp));
                    server.close();
                    if (bindSucc || this.m_asyncCloseOp) {
                        return;
                    }
                    if (curPort < endPort) {
                        curPort += 1;
                        tryBindNextPort();
                    } else {
                        resolve({error: MixSocket.ERROR.addressConflict});
                    }
                });
                server.listen(curPort, ip);
            }
            tryBindNextPort();
        });
    }

    _updateRouteTable(socket, remoteAddr, protocol, isRecv = true) {
        let now = TimeHelper.uptimeMS();
        let remoteEP = EndPoint.toString(remoteAddr);
        let routeTable = this.m_routeTable.get(remoteEP);
        if (!routeTable) {
            routeTable = {connections: []};
            this.m_routeTable.set(remoteEP, routeTable);
        }

        for (let connection of routeTable.connections) {
            if (connection.socket === socket) {
                if (isRecv) {
                    connection.lastRecvTime = now;
                } else {
                    connection.lastSendTime = now;
                }
                return {connection, isNew: false, routeTable};
            }
        }

        let newConnection = {
            socket,
            protocol,
            startTime: now,
            lastRecvTime: isRecv? now : 0,
            lastSendTime: now,
        };
        if (protocol === MixSocket.PROTOCOL.tcp) {
            newConnection.recvQueue = [];
            newConnection.recvQueue.totalByteLength = 0;
            newConnection.sendQueue = [];
            newConnection.sendingLength = 0;
            newConnection.sendCallbacks = [];
            newConnection.sendWaiting = false;
        }
        routeTable.connections.push(newConnection);

        return {connection: newConnection, isNew: true, routeTable};
    }

    // 超时连接：在很长一段时间内没收到收据包，如果是新建连接（从没收到过数据包），从连接建立时间计算
    _isConnectionTimeout(connection, time) {
        return (time || TimeHelper.uptimeMS()) - (connection.lastRecvTime || connection.startTime) >= this.m_options.socketIdle;
    }

    // 有效连接：在一定时间段内收到过数据包，新建连接是无效的，因为对方可能就是不在线
    _isConnectionValid(connection, time) {
        return (time || TimeHelper.uptimeMS()) - connection.lastRecvTime < this.m_options.socketIdle;
    }

    // 清理路由表
    // 允许传入 自定义的清理connection的方法
    _cleanRouteTable(cleanConnection = null) {
        let now = TimeHelper.uptimeMS();

        if ( !cleanConnection ) {
            cleanConnection = connections => {
                for (let i = 0; i < connections.length;) {
                    let connection = connections[i];
                    // 如果超过 设定时间 没有收到新包
                    // 就把connection 清除, 并发送'end'
                    if (this._isConnectionTimeout(connection, now)) {
                        connections.splice(i, 1);
                        if (connection.protocol === MixSocket.PROTOCOL.tcp) {
                            connection.socket.destroy();
                            connection.___timeout_flag = 0x133A129;
                        }
                    } else {
                        i++;
                    }
                }
            }
        }

        // 清理路由表中没有 connection的远程ep
        Array.from(this.m_routeTable).filter(val => {
            const [, routeTable] = val;
            cleanConnection(routeTable.connections);
            return routeTable.connections.length === 0;
        }).forEach(val => {
            const [remoteEP] = val;
            this.m_routeTable.delete(remoteEP);
        });
    }


    _onTCPClientCreated(clientSocket, localAddr, remoteAddr, isAccept, isReuseListener) {
        if (clientSocket.destroyed) {
            return;
        }

        localAddr.protocol = MixSocket.PROTOCOL.tcp;
        remoteAddr.protocol = MixSocket.PROTOCOL.tcp;
        clientSocket.isAccept = isAccept;
        clientSocket.isReuseListener = (isReuseListener || isAccept);
        this._updateRouteTable(clientSocket, remoteAddr, MixSocket.PROTOCOL.tcp, true);

        clientSocket.__trace = {
            nextSeq: 0,
            pendingSeqList: [],
            identify: 20160809,
        };
        clientSocket.on('data', buffer => {
            if (!clientSocket.destroyed) {
                let {connection} = this._updateRouteTable(clientSocket, remoteAddr, MixSocket.PROTOCOL.tcp);
                setImmediate(() => this._tcpMessage(connection, buffer, remoteAddr, localAddr));
            }
        });
        clientSocket.on('error', error => {
                setImmediate(() => this._onSocketError(error, clientSocket, localAddr, remoteAddr, MixSocket.PROTOCOL.tcp));
                clientSocket.destroy();
            });
        clientSocket.once('end', () => setImmediate(() => clientSocket.destroy()));
        clientSocket.once('close', () => setImmediate(() => 
            this._onSocketClosed(clientSocket, localAddr, remoteAddr, MixSocket.PROTOCOL.tcp)
        ));

        clientSocket.on('drain', () => {
            this.emit(MixSocket.EVENT.drain, clientSocket, remoteAddr, MixSocket.PROTOCOL.tcp);
        });
    }

    _onSocketClosed(socket, localAddr, remoteAddr, protocol) {
        this.emit(MixSocket.EVENT.closeSocket, socket, localAddr, remoteAddr, protocol);

        let cleanConnection = connections => {
            for (let i = 0; i < connections.length;) {
                if (connections[i].socket === socket) {
                    let con = connections.splice(i, 1);
                    con.___close_flag = 0x133A129;
                } else {
                    i++;
                }
            }
        }

        if (remoteAddr) {
            // 清理连接相关历史记录
            let remoteEP = EndPoint.toString(remoteAddr);
            let routeTable = this.m_routeTable.get(remoteEP);
            if (routeTable) {
                cleanConnection(routeTable.connections);
                if (routeTable.connections.length === 0) {
                    this.m_routeTable.delete(remoteEP)
                }
            }
        } else {
            // 监听socket关闭
            let localEP = EndPoint.toString(localAddr);
            if (protocol === MixSocket.PROTOCOL.udp) {
                // 清理历史收包记录
                this._cleanRouteTable(cleanConnection);
                this.m_udpListeners.delete(localEP);
            } else {
                assert(protocol === MixSocket.PROTOCOL.tcp);
                this.m_tcpListeners.delete(localEP);
            }
            // 全部监听socket关闭
            if (this.m_udpListeners.size === 0 && this.m_udpListeners.asyncOps.size === 0 &&
                this.m_tcpListeners.size === 0 && this.m_tcpListeners.asyncOps.size === 0 &&
                this.m_routeTable.size === 0) {
                    this.emit(MixSocket.EVENT.close);
            }
        }
    }

    _onSocketError(error, socket, localAddr, remoteAddr, protocol) {
        this.emit(MixSocket.EVENT.errorSocket, error, socket, localAddr, remoteAddr, protocol);
    }

    _connectTCP(remoteAddr, localAddr, isReuseListener) {
        let localEP = localAddr? EndPoint.toString(localAddr, EndPoint.PROTOCOL.tcp) : `4@${EndPoint.CONST_IP.zeroIPv4}@0@t`;
        let remoteEP = EndPoint.toString(remoteAddr);
        let connectionKey = `${localEP}-${remoteEP}`;
        let callbacks = null;

        let onComplete = (result) => {
            callbacks.forEach(cb => setImmediate(() => cb(result)));
            this.m_pendingConnections.delete(connectionKey);
        }

        let connectTCP = () => {
            let socket = new net.Socket();
            if (!socket) {
                onComplete({error: MixSocket.ERROR.socketCreateFailed});
                return;
            }
    
            let onConnect = null;
            let onError = null;
            let onClose = null;
            onConnect = () => {
                localAddr = socket.address();
                localAddr.protocol = MixSocket.PROTOCOL.tcp;
                // console.log(`connect succ ${connectionKey}`);
                this._onTCPClientCreated(socket, localAddr, remoteAddr, false, isReuseListener);

                socket.removeListener('error', onError);
                socket.removeListener('close', onClose);
                onComplete({error: MixSocket.ERROR.success, socket});
            };
            onClose = () => {
                socket.removeListener('connect', onConnect);
                socket.removeListener('error', onError);
                onComplete({error: MixSocket.ERROR.tcpConnectFailed});
            };
            onError = (err) => {
                socket.removeListener('connect', onConnect);
                socket.removeListener('close', onClose);
                onComplete({error: MixSocket.ERROR.tcpConnectFailed});
            };

            socket.once('connect', onConnect);
            socket.once('error', onError);
            socket.once('close', onClose);

            let opt = {
                port: remoteAddr.port,
            };
            // '0.0.0.0'启用默认的localhost地址
            opt.host = mapZeroIP(remoteAddr);
            if (localAddr) {
                if (localAddr.address && !EndPoint.isZero(localAddr)) {
                    opt.localAddress = localAddr.address;
                }
                if (localAddr.port) {
                    opt.localPort = localAddr.port;
                }
            }
            socket.connect(opt);
        }
        
        return new Promise(resolve => {
            let callback = (result) => {
                resolve(result);
            }
    
            let connectInfo = this.m_pendingConnections.get(connectionKey);
            if (connectInfo) {
                connectInfo.callbacks.push(callback);
            } else {
                callbacks = [callback];
                connectInfo = {callbacks};
                this.m_pendingConnections.set(connectionKey, connectInfo);
                connectTCP();
            }
        });
    }
}

MixSocket.EVENT = {
    drain: 'drain', // on(socket, remoteAddr, protocol)
    close: 'close', // 全部socket都close，on()
    closeSocket: 'closeSocket', // 一个socket关闭，on(socket, localAddr, remoteAddr, protocol)
    errorSocket: 'errorSocket', // 一个socket出错，on(error, socket, localAddr, remoteAddr, protocol)
};

MixSocket.PROTOCOL = EndPoint.PROTOCOL;
MixSocket.version = 'v2';

MixSocket.ERROR = {
    success: 0,
    statusConflict: 1,
    invalidArgs: 2,
    addressConflict: 3,
    socketNotFound: 4,
    socketCreateFailed: 5,
    tcpConnectFailed: 6,
    dataCannotParsed: 7,

    toString(error) {
        if (!error) {
            error = MixSocket.ERROR.success;
        }
        switch (error) {
            case MixSocket.ERROR.success: return 'success';
            case MixSocket.ERROR.statusConflict: return 'invalid operation in current status';
            case MixSocket.ERROR.invalidArgs: return 'invalid args';
            case MixSocket.ERROR.addressConflict: return 'ip/port conflict';
            case MixSocket.ERROR.socketNotFound: return 'socket not found';
            case MixSocket.ERROR.socketCreateFailed: return 'cannot create socket';
            case MixSocket.ERROR.tcpConnectFailed: return 'tcp connect failed';
            case MixSocket.ERROR.dataCannotParsed: return 'data cannot parsed';
            default: return `unknown-${error}`;
        }
    }
}

module.exports = MixSocket;

if (require.main === module) {
    async function runTest() {
        let udpProcess1 = (socket, buffer, remoteAddr, localAddr) => {
            console.log(`peer1 Message received: ${buffer}, from ${EndPoint.toString(remoteAddr)} to ${EndPoint.toString(localAddr)}, protocol is udp`);
            return [0, buffer.length];
        }
        let tcpProcess1 = (socket, bufferArray, remoteAddr, localAddr) => {
            let buffer = null;
            if (bufferArray.length === 1) {
                buffer = bufferArray[0];
            } else {
                buffer = Buffer.concat(bufferArray);
            }
            console.log(`peer1 Message received: ${buffer}, from ${EndPoint.toString(remoteAddr)} to ${EndPoint.toString(localAddr)}, protocol is tcp`);
            return [0, buffer.length];
        }
        let peer1 = new MixSocket(udpProcess1, tcpProcess1);
        let udpListenerOp1 = peer1.listen(['127.0.0.1', '192.168.100.148', '192.168.100.500', '0.0.0.0', 'fe80::58c5:91b6:c858:6401%13'], 7000, 10, MixSocket.PROTOCOL.udp);
        let tcpListenerOp1 = peer1.listen(['127.0.0.1', '192.168.100.148', '192.168.100.500', '0.0.0.0', 'fe80::58c5:91b6:c858:6401%13'], 8000, 10, MixSocket.PROTOCOL.tcp);

        peer1.on(MixSocket.EVENT.errorSocket, (error, socket, localAddr, remoteAddr, protocol) => {
            console.log(`peer1 socket error:${error.message}: from ${EndPoint.toString(localAddr)} to ${remoteAddr? EndPoint.toString(remoteAddr) : ''}, protocol is ${protocol}`);
        });
        peer1.on(MixSocket.EVENT.closeSocket, (socket, localAddr, remoteAddr, protocol) => {
            console.log(`peer1 socket close: from ${EndPoint.toString(localAddr)} to ${remoteAddr? EndPoint.toString(remoteAddr) : ''}, protocol is ${protocol}`);
        });
        peer1.on(MixSocket.EVENT.close, () => {
            console.log(`peer1 close`);
        });

        let udpProcess2 = (socket, buffer, remoteAddr, localAddr) => {
            console.log(`peer2 Message received: ${buffer}, from ${EndPoint.toString(remoteAddr)} to ${EndPoint.toString(localAddr)}, protocol is udp`);
            return [0, buffer.length];
        }
        let tcpProcess2 = (socket, buffer, remoteAddr, localAddr) => {
            console.log(`peer2 Message received: ${buffer}, from ${EndPoint.toString(remoteAddr)} to ${EndPoint.toString(localAddr)}, protocol is tcp`);
            return [0, buffer.length];
        }
        let peer2 = new MixSocket(udpProcess2, tcpProcess2);
        let udpListenerOp2 = peer2.listen(['127.0.0.1', '192.168.100.148', '192.168.100.500', '0.0.0.0', 'fe80::58c5:91b6:c858:6401%13'], 9000, 10, MixSocket.PROTOCOL.udp);
        let tcpListenerOp2 = peer2.listen(['127.0.0.1', '192.168.100.148', '192.168.100.500', '0.0.0.0', 'fe80::58c5:91b6:c858:6401%13'], 10000, 10, MixSocket.PROTOCOL.tcp);

        peer2.on(MixSocket.EVENT.errorSocket, (error, socket, localAddr, remoteAddr, protocol) => {
            console.log(`peer2 socket error:${error.message}: from ${EndPoint.toString(localAddr)} to ${remoteAddr? EndPoint.toString(remoteAddr) : ''}, protocol is ${protocol}`);
        });
        peer2.on(MixSocket.EVENT.closeSocket, (socket, localAddr, remoteAddr, protocol) => {
            console.log(`peer2 socket close: from ${EndPoint.toString(localAddr)} to ${remoteAddr? EndPoint.toString(remoteAddr) : ''}, protocol is ${protocol}`);
        });
        peer2.on(MixSocket.EVENT.close, () => {
            console.log(`peer2 close`);
        });

        await Promise.all([udpListenerOp1, tcpListenerOp1, udpListenerOp2, tcpListenerOp2]);

        peer2.send(Buffer.from('hello world'), peer1.eplist);
        await new Promise(resolve => setTimeout(() => resolve(), 1000));

        peer1.send(Buffer.from('hello world'), peer2.eplist);
        await new Promise(resolve => setTimeout(() => resolve(), 1000));
                
        peer1.close();
        peer2.close();
    }

    runTest();
}