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

const Base = require('../../base/base.js');
const msgpack = require('msgpack-lite');

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

const HEADER_LENGTH = 36;

const CommandType = {
    FIND_PEER_REQ: 0x51,
    FIND_PEER_RESP: 0x52,
    UPDATE_VALUE_REQ: 0x53,
    UPDATE_VALUE_RESP: 0x54,
    FIND_VALUE_REQ: 0x55,
    FIND_VALUE_RESP: 0x56,
    PING_REQ: 0x57,
    PING_RESP: 0x58,
    HANDSHAKE_REQ: 0x59,
    HANDSHAKE_RESP: 0x5A,
    HOLE_CALL_REQ: 0x5B,
    HOLE_CALL_RESP: 0x5C,
    HOLE_CALLED_REQ: 0x5D,
    HOLE_CALLED_RESP: 0x5E,
    BROADCAST_EVENT_REQ: 0x5F,
    BROADCAST_EVENT_RESP: 0x60,
    PACKAGE_PIECE_REQ: 0x61,
    PACKAGE_PIECE_RESP: 0x62,
    COMBINE_PACKAGE: 0x63,

    isResp(cmdType) {
        return !(cmdType & 0x1);
    },

    isValid(cmdType) {
        return cmdType >= CommandType.FIND_PEER_REQ && cmdType <= CommandType.COMBINE_PACKAGE;
    },

    toString(cmdType) {
        const key = Object.keys(CommandType)
              .filter(key => {
                  let val = CommandType[key];
                  return val == cmdType;
              }).reduce((str,key) => key, '');

        return key == '' ? `DHTCMD:Unknown_${cmdType}`: `DHTCMD:${key}`;
    },
}

class DHTPackage {
    constructor(cmdType, owner) {
        this.m_common = {
            'cmdType': cmdType,
            'appid': 0,
            'src': {
                'hash': undefined,
                'peerid': undefined,
                'eplist': null,
                'services': null,
                'additionalInfo': null,
            },
            'dest': {
                'hash': undefined,
                'peerid': undefined,
                'ep': null,
            },
            'seq': 0,
            'ackSeq': 0,
            'ttl': 0,
            'packageID': 0,
            'nodes': null,
        };

        this.m_body = null;
        if (owner) {
            this.m_owner = owner;
            this.m_common.appid = owner.appid || 0;
            this.m_common.seq = owner.genSeq();
        }
    }

    get appid() {
        return this.m_common.appid;
    }
    
    get cmdType() {
        return this.m_common.cmdType;
    }

    get seq() {
        return this.m_common.seq;
    }

    updateSeq() {
        LOG_ASSERT(this.m_owner);
        if (this.m_owner) {
            this.m_common.seq = this.m_owner.genSeq();
        }
    }

    get ackSeq() {
        return this.m_common.ackSeq;
    }

    fillCommon(srcPeerInfo, destPeerInfo, recommandNodes = null) {
        this.m_common.src.peerid = srcPeerInfo.peerid;
        this.m_common.src.eplist = srcPeerInfo.eplist;
        this.m_common.src.services = srcPeerInfo.services;
        this.m_common.src.additionalInfo = srcPeerInfo.additionalInfo;
        this.m_common.src.hash = srcPeerInfo.hash;
        this.m_common.src.onlineDuration = srcPeerInfo.onlineDuration;
        this.m_common.src.natType = srcPeerInfo.natType;
        if (srcPeerInfo.inactive) {
            this.m_common.src.inactive = true;
        } else {
            delete this.m_common.src.inactive;
        }
        this.m_common.dest.peerid = destPeerInfo.peerid;
        this.m_common.dest.hash = destPeerInfo.hash;
        this.m_common.dest.ep = destPeerInfo.ep;
        if (recommandNodes && recommandNodes.length > 0) {
            this.m_common.nodes = recommandNodes;
        }
    }

    get common() {
        return this.m_common;
    }

    get body() {
        return this.m_body;
    }

    set body(newValue) {
        this.m_body = newValue;
    }

    get src() {
        return this.m_common.src;
    }

    get dest() {
        return this.m_common.dest;
    }

    get servicePath() {
        if (this.m_body) {
            return this.m_body.servicePath;
        }
        return undefined;
    }

    get nodes() {
        return this.m_common.nodes;
    }

    set nodes(newValue) {
        this.m_common.nodes = newValue;
    }

    decodeBody(bodyBuffer) {
        if (bodyBuffer.length > 0) {
            this.m_body = null;
            try {
                this.m_body = msgpack.decode(bodyBuffer);
            } catch (error) {
                
            }
        } else {
            this.m_body = {};
        }
        this._fillDefaultBodyField();
        return this.m_body;
    }

    checkCommon() {
        LOG_ASSERT(this.m_common.src.hash && typeof this.m_common.src.hash === 'number',
            `Package(${CommandType.toString(this.m_common.cmdType)}) field(src.hash:number) not filled.`);
        LOG_ASSERT(this.m_common.dest.hash && typeof this.m_common.dest.hash === 'number',
            `Package(${CommandType.toString(this.m_common.cmdType)}) field(dest.hash:number) not filled.`);
            
        if (this.m_common.cmdType !== CommandType.PACKAGE_PIECE_REQ) {
            LOG_ASSERT(this.m_common.src.peerid && typeof this.m_common.src.peerid === 'string',
                `Package(${CommandType.toString(this.m_common.cmdType)}) field(src.peerid:string) not filled.`);
            LOG_ASSERT(this.m_common.dest.peerid && typeof this.m_common.dest.peerid === 'string',
                `Package(${CommandType.toString(this.m_common.cmdType)}) field(dest.peerid:string) not filled.`);
            if (CommandType.isResp(this.m_common.cmdType)) {
                LOG_ASSERT(typeof this.m_common.ackSeq === 'number' && this.m_common.ackSeq >= 0 && this.m_common.ackSeq <= 0xFFFFFFFF,
                    `Package(${CommandType.toString(this.m_common.cmdType)}) field(ackSeq:number) not filled.`);
            }

            if (this.m_common.nodes) {
                for (let peer of this.m_common.nodes) {
                    this._checkPeer(peer);
                }
            }
        }
    }

    checkBody() {
    }

    checkAllField() {
        this.checkCommon();
        this.checkBody();
    }

    _checkPeer(peer) {
        LOG_ASSERT(peer.id && typeof peer.id === 'string' && peer.id.length > 0,
            `Package(${CommandType.toString(this.m_common.cmdType)}) field(peer.id:string) not filled.`);
        LOG_ASSERT(peer.eplist && typeof peer.eplist === 'object' && typeof peer.eplist[0] === 'string',
            `Package(${CommandType.toString(this.m_common.cmdType)}) field(peer.eplist:array[string]) not filled.`);
    }

    _fillDefaultBodyField() {

    }
}

DHTPackage.HEADER_LENGTH = HEADER_LENGTH;

DHTPackage.CommandType = CommandType;
module.exports = DHTPackage;
