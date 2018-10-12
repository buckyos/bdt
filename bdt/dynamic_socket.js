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

/**
 * 动态分配新UDP端口用于通信
 * 主要起两点作用：
 * 1.管理历史分配端口，尽量拉长同一端口重复使用的时间间隔
 * 2.适配mixSocket接口，做到调用形式上的统一
 */
"use strict";
const dgram = require('dgram');
const {EndPoint, TimeHelper} = require('../base/util.js');
const baseModule = require('../base/base');
const blog = baseModule.blog;
const packageModule = require('./package');
const BDTPackage = packageModule.BDTPackage;
const BDT_ERROR = packageModule.BDT_ERROR;

// 对0.0.0.0地址替换为回环地址
function mapZeroIP(remoteAddr) {
    let result = remoteAddr.address;
    if (!result || EndPoint.isZero(remoteAddr)) {
        result = EndPoint.loopback(remoteAddr.protocol);
    }
    return result;
}

class DynamicSocket {
    constructor(packageProcess, options) {
        this.m_sockets = new Map(); // <port, {socket, lastSendTime, lastRecvTime}>

        this.m_options = {
            minPort: 40809,
            maxPort: 60809,
            portDeadTime: 600809,
        };

        if (options) {
            Object.assign(this.m_options, options);
        }

        this.m_nextPort = this.m_options.minPort;
        this.m_packageProcess = packageProcess;
    }

    destroy() {
        this.m_sockets.forEach((socketInfo, port) => {
            if (socketInfo.socket) {
                socketInfo.socket.close();
                socketInfo.socket = null;
            }
        });
        this.m_sockets.clear();
    }

    create() {
        return new Promise(resolve => {
            let bindSucc = false;
            let portCount = this.m_options.maxPort - this.m_options.minPort + 1;

            if (portCount <= 0) {
                resolve(null);
                return;
            }
            
            let tryBindNextPort = () => {
                let port = this._selectPort();
                if (!port) {
                    resolve(null);
                    return;
                }
        
                let socketInfo = {
                    socket: null,
                    lastSendTime: TimeHelper.uptimeMS(),
                    lastRecvTime: 0,
                };

                this.m_sockets.set(port, socketInfo);
                this.m_nextPort = (port + 1 - this.m_options.minPort) % portCount + this.m_options.minPort;

                let localAddr = {
                    family: EndPoint.FAMILY.IPv4,
                    address: EndPoint.CONST_IP.zeroIPv4,
                    port: port,
                    protocol: EndPoint.PROTOCOL.udp,
                };

                let socket = dgram.createSocket('udp4');
                socket.once('listening', () => {
                    socketInfo.socket = socket;
                    socket.on('message', (msg, rinfo) => {
                        let decoder = BDTPackage.createDecoder(msg.slice(0, rinfo.size));
                        if (decoder.decodeHeader() || decoder.decodeBody()) {
                            return;
                        }

                        let remoteAddr = {
                            address: rinfo.address,
                            port: rinfo.port,
                            family: rinfo.family,
                        };
                        remoteAddr.protocol = EndPoint.PROTOCOL.udp;
                        socketInfo.lastRecvTime = TimeHelper.uptimeMS();
                        setImmediate(() => this.m_packageProcess(socket, decoder, remoteAddr, localAddr));
                    });
                    
                    blog.info(`[dynsock]: socket bind udp ${socket.address().address}:${port} success`);
                    bindSucc = true;
                    let linfo = socket.address();
                    localAddr = {
                        address: linfo.address,
                        port: linfo.port,
                        family: linfo.family,
                    };
                    localAddr.protocol = EndPoint.PROTOCOL.udp;
                    resolve(socket);
                });
    
                socket.once('close', () => {
                    socket = null;
                    socketInfo.socket = null;
                    // this.m_sockets.delete(port);
                });
    
                socket.on('error', error => {
                    blog.debug(`[dynsock]: socket bind udp '0.0.0.0':${port} failed, error:${error}.`);
                    // this.m_sockets.delete(port);
                    // 可能已经被关闭，或者在bind过程中就error了
                    if (socketInfo.socket || !bindSucc) {
                        socket.close();
                        socketInfo.socket = null;
                    }
                    if (bindSucc) {
                        return;
                    }

                    tryBindNextPort();
                });
                socket.bind(port);
            }
            tryBindNextPort();
        });
    }

    send(pkg, eplist, options) {
        let socket = options.socket;
        let localAddr = null;
        try {
            localAddr = socket.address();
        } catch (error) {
            // socket可能已经关闭
            return BDT_ERROR.invalidState;
        }
        let socketInfo = this.m_sockets.get(localAddr.port);
        if (!socketInfo) {
            return BDT_ERROR.invalidArgs;
        }

        socketInfo.lastSendTime = TimeHelper.uptimeMS();

        let onPreSend = options.onPreSend;
        let onPostSend = options.onPostSend;

        eplist.forEach(ep => {
            let remoteAddr = EndPoint.toAddress(ep);
            if (!remoteAddr ||
                remoteAddr.protocol !== EndPoint.PROTOCOL.udp ||
                remoteAddr.family !== EndPoint.FAMILY.IPv4) {
                    return;
            }

            let buffer = pkg;
            if (onPreSend) {
                buffer = onPreSend(pkg, remoteAddr, socket, EndPoint.PROTOCOL.udp);
            }
            if (!buffer) {
                return;
            }

            if ((typeof buffer) != 'string' && !(buffer instanceof Buffer)) {
                buffer = JSON.stringify(buffer);
            }

            // 用系统默认地址（localhost）替换'0.0.0.0'
            socket.send(buffer, remoteAddr.port, mapZeroIP(remoteAddr),
                err => {
                    if (err) {
                        blog.error(`Send package error: ${err.message}`);
                    } else if (onPostSend) {
                        onPostSend(pkg, remoteAddr, socket, EndPoint.PROTOCOL.udp);
                    }
                });
        });
        return BDT_ERROR.success;
    }        

    releaseSocket(sock) {
        let localAddr = null;
        try {
            localAddr = sock.address();
        } catch (error) {
            // socket可能已经关闭
            return BDT_ERROR.invalidState;
        }
        let socketInfo = this.m_sockets.get(localAddr.port);
        if (!socketInfo) {
            return BDT_ERROR.invalidArgs;
        }

        this.m_sockets.delete(localAddr.port);
        if (socketInfo.socket) {
            socketInfo.socket.close();
            socketInfo.socket = null;
        }
        return BDT_ERROR.success;
    }

    _selectPort() {
        let now = TimeHelper.uptimeMS();

        if (this.m_sockets.size > 10) {
            let timeoutPorts = [];
            this.m_sockets.forEach((socketInfo, port) => {
                if (now - Math.max(socketInfo.lastSendTime, socketInfo.lastRecvTime) > this.m_options.portDeadTime) {
                    if (socketInfo.socket) {
                        socketInfo.socket.close();
                        socketInfo.socket = null;
                    }
                    timeoutPorts.push(port);
                }
            });
            timeoutPorts.forEach(port => this.m_sockets.delete(port));
        }

        let portCount = this.m_options.maxPort - this.m_options.minPort + 1;
        for (let next = this.m_nextPort; next < this.m_nextPort + portCount; next++) {
            let port = (next - this.m_options.minPort) % portCount + this.m_options.minPort;
            let socketInfo = this.m_sockets.get(port);
            if (!socketInfo) {
                return port;
            } else if (now - Math.max(socketInfo.lastSendTime, socketInfo.lastRecvTime) > this.m_options.portDeadTime) {
                if (socketInfo.socket) {
                    socketInfo.socket.close();
                    socketInfo.socket = null;
                }
                this.m_sockets.delete(port);
                return port;
            }
        }
        return 0;
    }
}

module.exports = DynamicSocket;