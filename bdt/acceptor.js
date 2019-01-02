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
const assert = require('assert');
const BDTStack = require('./stack');
const BDTConnection = require('./connection');
const packageModule = require('./package');
const BDT_ERROR = packageModule.BDT_ERROR;
const BDTPackage = packageModule.BDTPackage;
const baseModule = require('../base/base');
const blog = baseModule.blog;

class BDTAcceptor extends EventEmitter {
    /*
        options:
            vport:number vport 
            allowHalfOpen: true or false
    */
    constructor(stack, options) {
        super();
        this.m_stack = stack;
        this.m_remoteMap = {};
        this.m_state = BDTAcceptor.STATE.init;
        this.m_vport = parseInt(options.vport);

        this.m_options = {
            // 是否允许半开连接：
            // 如果置true，在收到'end'事件后，依旧可以向对端发送数据，直到不再需要connection后手动调用close关闭连接；
            // 默认值为false，在收到对端发来的'fin'包后自动回复一个'fin'关闭连接。
            allowHalfOpen: false,
        };

        if (options && options.allowHalfOpen) {
            this.m_options.allowHalfOpen = true;
        }
    }

    listen() {
        if (this.m_stack.state !== BDTStack.STATE.created) {
            blog.warn(`[BDT]: listen on acceptor when stack not created`);
            return BDT_ERROR.invalidState;
        }
        if (this.m_state !== BDTAcceptor.STATE.init) {
            blog.warn(`[BDT]: listen on acceptor not in init state`);
            return BDT_ERROR.invalidState;
        }
        let err = this.m_stack._refAcceptor(this.m_vport, this);
        if (err) {
            blog.error(`[BDT]: acceptor bind vport ${this.m_vport} failed for ${BDT_ERROR.toString(err)}`);
            return err;
        } 
        blog.info(`[BDT]: acceptor begin listen on vport ${this.m_vport}`);
        this.m_state = BDTAcceptor.STATE.listening;
        return BDT_ERROR.success;
    }

    close() {
        if (this.m_state === BDTAcceptor.STATE.listening) {
            if (!Object.keys(this.m_remoteMap).length) {
                this.m_stack._unrefAcceptor(this.m_vport, this);
                this.m_state === BDTAcceptor.STATE.closed;
                setImmediate(()=>{this.emit(BDTAcceptor.EVENT.close);});
            } else {
                for (let [, portMap] of Object.entries(this.m_remoteMap)) {
                    for (let [, connection] of Object.entries(portMap)) {
                        connection.close();
                    }
                }
                this.m_state = BDTAcceptor.STATE.closing;
            }
        }
    }

    get state() {
        return this.m_state;
    }

    get vport() {
        return this.m_vport;
    }

    _onPackage(decoder, remoteSender, isDynamic) {
        let header = decoder.header;
        if (header.cmdType === BDTPackage.CMD_TYPE.calledReq
            || header.cmdType === BDTPackage.CMD_TYPE.syn) {
            if (this.m_state !== BDTAcceptor.STATE.listening) {
                return;
            }
            if (typeof decoder.body.src !== 'string' || decoder.body.src.length === 0) {
                return;
            }
            let err = BDT_ERROR.success;
            let remote = {
                peerid: decoder.body.src,
                peeridHash: header.src.peeridHash,
                vport: header.src.vport,
                sessionid: header.sessionid,
            };
            let connection = this._getConnectionByRemote(remote);
            if (!connection) {
                [connection, err] = this._createConnection(remote);
                if (err) {
                    return ;
                }
                setImmediate(() => this.emit(BDTAcceptor.EVENT.accept, connection));
            }
            connection._onPackage(decoder, remoteSender, isDynamic);
        } else {
            assert(false, 'should not reach here.');
        } 
    }

    _createConnection(remote) {
        let connection = new BDTConnection(this.m_stack, this.m_options);
        let params = {
            acceptor: this,
            remote: remote,
        };
        let err = connection._createFromAcceptor(params);
        assert(!err);
        if (!err) {
            this._refRemote(remote, connection);
            connection.once(BDTConnection.EVENT.error, ()=>{
                connection.close();
            });
            return [connection, err];
        }
        return [null, err];
    }

    _getConnectionByRemote(remote) {
        const remoteMap = this.m_remoteMap;
        const portMap = remoteMap[remote.peerid];
        if (!portMap) {
            return null;
        }
        return portMap[remote.sessionid];
    }

    _refRemote(remote, connection) {
        let remoteMap = this.m_remoteMap;
        let portMap = remoteMap[remote.peerid];
        if (!portMap) {
            portMap = {};
            remoteMap[remote.peerid] = portMap;
        }
        assert(!portMap[remote.sessionid]);
        portMap[remote.sessionid] = connection;
    }

    _unrefRemote(remote, connection) {
        let remoteMap = this.m_remoteMap;
        let portMap = remoteMap[remote.peerid];
        if (!portMap) {
            return;
        }
        if (portMap[remote.sessionid] === connection) {
            delete portMap[remote.sessionid];
        }
        if (!Object.keys(portMap).length) {
            delete remoteMap[remote.peerid];
        }
        if (this.m_state === BDTAcceptor.STATE.closing) {
            if (!Object.keys(remoteMap).length) {
                this.m_stack._unrefPort(this.m_vport, this);
                this.m_state = BDTAcceptor.STATE.closed;
                setImmediate(()=>{this.emit(BDTAcceptor.EVENT.close);});
            }
        }
    }

    _onConnection(connection) {
        // connection.removeAllListeners(BDTConnection.EVENT.error);
        setImmediate(() => this.emit(BDTAcceptor.EVENT.connection, connection));
    }

    /* events
     connection(connection:BDTConnection)
    */
}

BDTAcceptor.STATE = {
    init: 0,
    listening: 1,
    closing: 2,
    closed: 10,
}

BDTAcceptor.EVENT = {
    accept: 'accept', // 对方正在尝试连接，还没成功，一般调试阶段用
    connection: 'connection',
    close: 'close',
}

module.exports = BDTAcceptor;