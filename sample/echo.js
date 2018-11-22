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

// BDT例程，实现简单的服务器向客户端原样回传数据的功能

"use strict";

const {P2P} = require('../index');
const DHTAPPID = require('../base/dhtappid');
const SERVICEID = require('../base/serviceid');

class BDTEcho {
    constructor(peerinfo) {
        this.m_peerinfo = peerinfo;
        this.m_p2p = null;
        this.m_bdtStack = null;
        this.m_connect = null;
        this.m_sendBuffer = Buffer.from(`hello! I'm ${this.m_peerinfo.peerid}, please echo me.`);
        this.m_recvLength = 0;
    }

    async start() {
        // 这里SN信息直接写入参数中
        // 如果只知道目标DHT网络中的某些入口节点信息，而没有SN信息，需要先从目标DHT网络中搜索到SN入口信息：
        // dht.getValue(SERVICEID.sn, 'random', GetValueFlag.UpdateLatest);
        const param = this._makeStackParam();
        let {result, p2p, bdtStack} = await P2P.create4BDTStack(param);
        if (result) {
            console.log(`stack start failed:${result}`);
        } else {
            console.log(`stack start success.`);
        }
        this.m_bdtStack = bdtStack;
        this.m_p2p = p2p;
        p2p.joinDHT(param.dhtEntry, {manualActiveLocalPeer: false, dhtAppID: DHTAPPID.none, asDefault: true});
    }

    connect(remotePeerid, vport) {
        let connection = this.m_bdtStack.newConnection();
        connection.bind(null);
        
        console.log(`[${this.m_peerinfo.peerid} - ${remotePeerid}] begin connect.`);
        connection.connect({peerid: remotePeerid, vport});
        
        connection.on(P2P.Connection.EVENT.error, error => this._onClientError(connection, error));
        connection.on(P2P.Connection.EVENT.close, () => this._onClientClose(connection));
        connection.on(P2P.Connection.EVENT.connect, () => this._onClientConnect(connection));
        return connection;
    }

    listen(vport) {
        this.m_acceptor = this.m_bdtStack.newAcceptor({vport});
        this.m_acceptor.listen();
        console.log(`[${this.m_peerinfo.peerid}:${vport}] begin listen.`);
        
        this.m_acceptor.on(P2P.Acceptor.EVENT.close, () => this._onAcceptorClose(this.m_acceptor));

        this.m_acceptor.on(P2P.Acceptor.EVENT.connection, 
            (connection)=>{
        
                this._onAcceptorConnect(this.m_acceptor, connection);
            });
        this.m_acceptor.on('error', 
            ()=>{
                this._onAcceptorError(this.m_acceptor);
            });

        return this.m_acceptor;
    }

    close() {
        return this.m_p2p.close();
    }

    _makeStackParam() {
        const { peerid, udpPort, tcpPort } = this.m_peerinfo

        let param = {
            peerid: peerid,
            dhtEntry: [],
        };

        if (udpPort || udpPort === 0) {
            param.udp = {
                addrList: ['0.0.0.0'], 
                initPort: udpPort,
                maxPortOffset: 10,
            };
        }

        if (tcpPort || tcpPort === 0) {
            param.tcp = {
                addrList: ['0.0.0.0'], 
                initPort: tcpPort,
                maxPortOffset: 10,
            };
        }
        this.m_peerinfo.seedPeers.forEach(seedPeer => param.dhtEntry.push(seedPeer));
        return param;
    }

    _onClientConnect(connection) {
        console.log(`[${this.m_peerinfo.peerid} - ${connection.remote.peerid}] connected.`);
        console.log(`${this.m_peerinfo.peerid} send data:${this.m_sendBuffer} to ${connection.remote.peerid}`);
        connection.send(this.m_sendBuffer);
        connection.on(P2P.Connection.EVENT.data, buffers => {
                buffers.forEach(buffer => {
                    console.log(`${this.m_peerinfo.peerid} receive data:${buffer} from ${connection.remote.peerid}`);
                    let sendBuffer = this.m_sendBuffer;
                    let matchSendPos = this.m_recvLength % sendBuffer.length;
                    let matchSendEndPos = Math.min(matchSendPos + buffer.length, sendBuffer.length);
                    let matchSendBuffer = this.m_sendBuffer.slice(matchSendPos, matchSendEndPos);
                    if (Buffer.compare(buffer.slice(0, matchSendEndPos - matchSendPos), matchSendBuffer) !== 0) {
                        console.error(`echo package error.`);
                    }
                    this.m_recvLength += buffer.length;
                });
                connection.close();
            }
        );
    }
    
    _onClientClose(connection) {
        console.log(`[${this.m_peerinfo.peerid} - ${connection.remote.peerid}] connection closed.`);
    }

    _onClientError(connection, error) {
        console.error(`[${this.m_peerinfo.peerid} - ${connection.remote.peerid}] error:${error}`);
    }

    _onAcceptorConnect(acceptor, connection) {
        console.log(`[${connection.remote.peerid} - ${this.m_peerinfo.peerid}] connection accepted.`);
        connection.on(P2P.Connection.EVENT.error, error => {
            console.log(`[${connection.remote.peerid} - ${this.m_peerinfo.peerid}] connection error.`);
        });
        connection.on(P2P.Connection.EVENT.close, () => {
            console.log(`[${connection.remote.peerid} - ${this.m_peerinfo.peerid}] connection closed.`);
        });
        connection.on(P2P.Connection.EVENT.data, buffers =>
            buffers.forEach(buffer => {
                if (buffer.length > 0) {
                    console.log(`${this.m_peerinfo.peerid} receive data:${buffer} from ${connection.remote.peerid}`);
                    connection.send(buffer)
                } else {
                    connection.close();
                }
            })
        );
    }

    _onAcceptorClose(acceptor) {
        console.error(`SERVER [${this.m_peerinfo.peerid}] closed`);
    }

    _onAcceptorError(acceptor) {
        console.error(`SERVER [${this.m_peerinfo.peerid}] error:${error}`);
    }
}

module.exports = BDTEcho;
