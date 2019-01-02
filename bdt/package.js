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
const msgpack = require('msgpack-lite');
const baseModule = require('../base/base');
const blog = baseModule.blog;
const {EndPoint, HashDistance, SequenceU32, TimeHelper} = require('../base/util');
const assert = require('assert');

const EMPTY_BUFFER = Buffer.allocUnsafe(0);

class BDTPackageDecoder {
    constructor(buffer, offset) {
        this.m_buffer = buffer;
        this.m_offset = offset;
        this.m_header = null;
        this.m_body = null;
        this.m_data = null;
    }

    decodeHeader() {
        const buffer = this.m_buffer;
        let header = {};
        let offset = this.m_offset;
        header.magic = buffer.readUInt16LE(offset);
        offset += 2;
        header.version = buffer.readUInt16LE(offset);
        offset += 2;
        header.cmdType = buffer.readUInt16LE(offset);
        offset += 2;
        if (!BDTPackage.CMD_TYPE.isValid(header.cmdType) || header.magic !== BDTPackage.MAGIC) {
            return BDT_ERROR.unmatchPackage;
        }
        header.totalLength = buffer.readUInt16LE(offset);
        offset += 2;
        if (buffer.length < header.totalLength) {
            return BDT_ERROR.outofSize;
        }
        // 暂时只处理同版本协议，要兼容其他版本协议需要另外考虑
        if (header.version !== BDTPackage.VERSION) {
            return BDT_ERROR.notSupportVersion;
        }
        header.headerLength = buffer.readUInt8(offset);
        offset++;
        let flags = buffer.readUInt16LE(offset);
        offset += 2;
        header.flags = flags;
        if ((flags & 0x1) === 0x1) {
            header.useTCP = true;
        }
        if ((flags & 0x2) === 0x2) {
            header.finalAck = true;
        }
        if ((flags & 0x4) === 0x4) {
            header.sack = true;
        }
        if ((flags & 0x8) === 0x8) {
            header.isResend = true;
        }
        header.bodyLength = buffer.readUInt16LE(offset);
        offset += 2;
        let src = {};
        let dest = {};
        src.vport = buffer.readUInt16LE(offset);
        offset += 2;
        dest.vport = buffer.readUInt16LE(offset);
        offset += 2;
        src.peeridHash = buffer.readUInt16LE(offset);
        offset += 2;
        dest.peeridHash = buffer.readUInt16LE(offset);
        offset += 2;
        header.src = src;
        header.dest = dest;
        header.seq = buffer.readUInt32LE(offset);
        offset += 4;
        header.ackSeq = buffer.readUInt32LE(offset);
        offset += 4;
        header.windowSize = buffer.readUInt16LE(offset);
        offset += 2;
        header.sessionid = buffer.readUInt32LE(offset);
        offset += 4;

        this.m_header = header;

        if (header.headerLength + header.bodyLength > header.totalLength) {
            return BDT_ERROR.invalidPackage;
        }
        if (header.headerLength + header.bodyLength !== header.totalLength) {
            // this.m_data = Buffer.from(this.m_buffer, header.bodyLength + header.headerLength);
            assert(this.m_buffer.length - this.m_offset >= this.m_header.totalLength, `pkg.length=${this.m_header.totalLength},buffer.length=${this.m_buffer.length},offset=${this.m_offset}`);
            this.m_data = this.m_buffer.slice(this.m_offset + header.headerLength + header.bodyLength, header.totalLength);
        }
        return BDT_ERROR.success;
    }

    decodeBody() {
        if (!this.m_body) {
            if (this.m_header.bodyLength) {
                assert(this.m_buffer.length >= this.m_header.totalLength, `pkg.length=${this.m_header.totalLength},buffer.length=${this.m_buffer.length}`);
                // let bodyBuffer = Buffer.from(this.m_buffer, this.m_offset + this.m_header.headerLength, this.m_header.bodyLength);
                let bodyBuffer = this.m_buffer.slice(this.m_offset + this.m_header.headerLength, this.m_offset + this.m_header.headerLength + this.m_header.bodyLength);
                let body = null;
                try {
                    body = msgpack.decode(bodyBuffer);
                } catch (error) {
                    return BDT_ERROR.invalidPackage;
                }
                this.m_body = body;
            } else {
                this.m_body = {};
            }
        }
        return BDT_ERROR.success;
    }

    get ackSeq() {
        if (this.m_data && !this.m_header.sack) {
            return  SequenceU32.add(this.m_header.seq, this.m_data.length);
        } else {
            return this.m_header.seq;
        }
    }

    get nextSeq() {
        return SequenceU32.add(this.ackSeq, 1);
    }

    get header() {
        const h = this.m_header;
        return h;
    }

    get body() {
        const b = this.m_body;
        return b;
    }

    get data() {
        return this.m_data;
    }
}


class BDTPackageEncoder {
    constructor() {
        this.m_header = {magic: BDTPackage.MAGIC, version: BDTPackage.VERSION, seq: 0, ackSeq: 0, windowSize: 0, sessionid: 0, useTCP: false, finalAck: false, sack: false, isResend: false};
        this.m_body = null;
        this.m_data = [];
        this.m_dataLength = 0;
        this.m_buffer = null;
    }

    encode() {
        if (!this.m_buffer) {
            let bodyBuf = this.m_body? msgpack.encode(this.m_body) : EMPTY_BUFFER;
            let len = BDTPackage.HEADER_LENGTH + bodyBuf.length;
            len += this.m_dataLength;
            let header = this.m_header;
            this.m_header.totalLength = len;
            this.m_header.bodyLength = bodyBuf.length;
            
            let flags = 0;
            if (header.useTCP) {
                flags |= 0x1;
            }
            // data包用于标记最后一个保底ack包，该包用于防止对方暂时没有数据发送，而最后一个数据包的ack又丢失；
            // 对方一直处于等待状态直到重传；长时间没有收到对方新的数据包，接收端会重复（间隔线性增加）发送finalAck
            if (header.finalAck) {
                flags |= 0x2;
            }
            // 接收端确认收到的乱序报文，data段填充的是[[fromSeq-1,toSeq-1),[fromSeq-2,toSeq-2), ... ,[fromSeq-n,toSeq-n)]数组
            if (header.sack) {
                flags |= 0x4;
            }
            // 发送端标记该报文是否是重发报文，接收端收到立即ack
            if (header.isResend) {
                flags |= 0x8;
            }
            this.m_header.flags = flags;
            let headerBuf = Buffer.alloc(BDTPackage.HEADER_LENGTH);
            let offset = 0;
            offset = headerBuf.writeUInt16LE(header.magic, offset);
            offset = headerBuf.writeUInt16LE(header.version, offset);
            offset = headerBuf.writeUInt16LE(header.cmdType, offset);
            offset = headerBuf.writeUInt16LE(header.totalLength, offset);
            offset = headerBuf.writeUInt8(BDTPackage.HEADER_LENGTH, offset);
            offset = headerBuf.writeUInt16LE(flags, offset);
            offset = headerBuf.writeUInt16LE(header.bodyLength, offset);
            offset = headerBuf.writeUInt16LE(header.src == null || header.src.vport == null ? 0 : header.src.vport, offset);
            offset = headerBuf.writeUInt16LE(header.dest == null || header.dest.vport == null ? 0 : header.dest.vport, offset);
            offset = headerBuf.writeUInt16LE(header.src == null || header.src.peeridHash == null ? 0 : header.src.peeridHash, offset);
            offset = headerBuf.writeUInt16LE(header.dest == null || header.dest.peeridHash == null ? 0 : header.dest.peeridHash, offset);
            offset = headerBuf.writeUInt32LE(header.seq || 0, offset);
            offset = headerBuf.writeUInt32LE(header.ackSeq || 0, offset);
            offset = headerBuf.writeUInt16LE(header.windowSize || 0, offset);
            offset = headerBuf.writeUInt32LE(header.sessionid, offset);
            this.m_buffer = Buffer.concat([headerBuf, bodyBuf, ...this.m_data]);
            assert(header.magic === BDTPackage.MAGIC && header.version === BDTPackage.VERSION && header.totalLength === this.m_buffer.length,
                `totalLength:${header.totalLength},buffer.length:${this.m_buffer.length},magic:${header.magic},version:${header.version}`);
        }
    }

    get header() {
        return this.m_header;
    }

    get body() {
        if (!this.m_body) {
            this.m_body = {};
        }
        return this.m_body;
    }

    get data() {
        return Buffer.concat(this.m_data);
    }

    addData(buffers) {
        this.m_data.push(...buffers);
        for (const buffer of buffers) {
            this.m_dataLength += buffer.length;
        }
    }

    get dataLength() {
        return this.m_dataLength;
    }

    get buffer() {
        const b = this.m_buffer;
        return b;
    }

    get ackSeq() {
        if (this.m_dataLength && !this.m_header.sack) {
            return  SequenceU32.add(this.m_header.seq, this.m_dataLength);
        } else {
            return this.m_header.seq;
        }
    }

    get nextSeq() {
        return  SequenceU32.add(this.ackSeq, 1);
    }

    change() {
        if (this.m_buffer) {
            this.m_buffer = null;
        }
    }
}

class BDTPackage {
    static createDecoder(buffer, offset=0) {
        return new BDTPackageDecoder(buffer, offset);
    }

    static createEncoder() {
        return new BDTPackageEncoder();
    }

    static createSender(mixSocket, socket, remoteEPList) {
        return new BDTPackageSender(mixSocket, socket, remoteEPList);
    }

    static hashPeerid(peerid) {
        return HashDistance.hash(peerid, 16);
    }
}

BDTPackage.HEADER_LENGTH = 35;
BDTPackage.MAGIC = 0x8083;
BDTPackage.VERSION = 0x102;

BDTPackage.CMD_TYPE = {
    pingReq: 0x10,
    pingResp: 0x11,
    callReq: 0x12,
    callResp: 0x13,
    calledReq: 0x14,
    calledResp: 0x15,
    sn2snReq: 0x16,
    sn2snResp: 0x17,
    syn: 0x20,
    synAck: 0x21,
    synAckAck: 0x22,
    data: 0x30,
    heartbeat: 0x32,
    heartbeatResp: 0x33,
    fin: 0x34,

    isValid(cmdType) {
        return cmdType >= BDTPackage.CMD_TYPE.pingReq && cmdType <= BDTPackage.CMD_TYPE.fin;
    },

    toString(cmdType) {
        switch(cmdType) {
            case BDTPackage.CMD_TYPE.pingReq:
                return 'SNCMD:PING_REQ';
            case BDTPackage.CMD_TYPE.pingResp:
                return 'SNCMD:PING_RESP';
            case BDTPackage.CMD_TYPE.callReq:
                return 'SNCMD:CALL_REQ';
            case BDTPackage.CMD_TYPE.callResp:
                return 'SNCMD:CALL_RESP';
            case BDTPackage.CMD_TYPE.calledReq:
                return 'SNCMD:CALLED_REQ';
            case BDTPackage.CMD_TYPE.calledResp:
                return 'SNCMD:CALLED_RESP';
            case BDTPackage.CMD_TYPE.sn2snReq:
                return 'SNCMD:SN2SN_REQ';
            case BDTPackage.CMD_TYPE.sn2snResp:
                return 'SNCMD:SN2SN_RESP';
            case BDTPackage.CMD_TYPE.syn:
                return 'BDTCMD:SYN';
            case BDTPackage.CMD_TYPE.synAck:
                return 'BDTCMD:SYNACK';
            case BDTPackage.CMD_TYPE.synAckAck:
                return 'BDTCMD:SYNACKACK';
            case BDTPackage.CMD_TYPE.data:
                return 'BDTCMD:DATA';
            case BDTPackage.CMD_TYPE.heartbeat:
                return 'BDTCMD:HEARTBEAT';
            case BDTPackage.CMD_TYPE.heartbeatResp:
                return 'BDTCMD:HEARTBEATRESP';
            case BDTPackage.CMD_TYPE.fin:
                return 'BDTCMD:FIN';
            default:
                return `Unknown_${cmdType}`;
        }
    },
};

const BDT_ERROR = {
    success: 0,
    conflict: 1,
    invalidState: 2,
    timeout: 3,
    outofSize: 4,
    dhtError: 5,
    invalidPackage: 6,
    unmatchPackage: 7,
    invalidArgs: 8,
    tooMuchConnection: 9,
    notSupportVersion: 10,
    connectionBreak: 11,
    noSNFound: 12,
    snNotResp: 13,
    noAddressFromSN: 14,
    remoteNoSyn: 15,
    remoteNoHeartbeat: 16,
    toString(err) {
        return Object.keys(BDT_ERROR)
            .filter(key => {
                let val = BDT_ERROR[key];
                return val == err;
            }).reduce((str,key) => key, '')
    }
};

class BDTPackageSender {
    constructor(mixSocket, socket, remoteEPList) {
        this.m_mixSocket = mixSocket;
        this.m_socket = socket;
        this.m_remoteEPList = [... new Set(remoteEPList)];
        this.m_isResend = false;
        this.m_activeEP = null;
        this.m_abort = false;
        this.m_lastSendTime = 0;
        this.init();
    }

    init() {
        
    }

    postPackage(encoder, onPreSend = null, dropBusyTCP = true, timeout = 0) {
        if (!encoder.buffer) {
            encoder.encode();
        }
        const socket = this.m_socket;
        const header = encoder.header;
        let remoteEPList = this.m_remoteEPList;
        if (!this.m_isResend && this.m_activeEP) {
            remoteEPList = [this.m_activeEP];
        }

        let onPreSendInner = (packageBuffer, remoteAddr, socket, protocol) => {
            if (this.m_abort) {
                return null;
            }

            let localAddr = null;
            if (protocol === EndPoint.PROTOCOL.udp) {
                localAddr = socket.address();
            } else {
                localAddr = {
                    address: socket.localAddress,
                    port: socket.localPort,
                }
            }

            blog.debug(`[BDT]: post ${BDTPackage.CMD_TYPE.toString(header.cmdType)} from ${localAddr.address}:${localAddr.port} to remote ${EndPoint.toString(remoteAddr)},sessionid:${header.sessionid}, seq:${header.seq},ackSeq:${header.ackSeq},flags:${header.flags}`);

            assert(encoder.header.totalLength === packageBuffer.length, `totalLength:${encoder.header.totalLength},buffer.length:${packageBuffer.length}`);
            if (onPreSend) {
                onPreSend(packageBuffer, remoteAddr, socket, protocol);
            }
            return packageBuffer;
        };

        this.m_lastSendTime = TimeHelper.uptimeMS();

        let options = {
            ignoreCache: this.m_isResend,
            socket,
            onPreSend: onPreSendInner,
            dropBusyTCP,
            timeout,
        };
        return this.m_mixSocket.send(encoder.buffer, remoteEPList, options);
    }

    get mixSocket() {
        return this.m_mixSocket;
    }

    get socket() {
        return this.m_socket;
    }

    set socket(newSocket) {
        this.m_socket = newSocket;
    }

    get remoteEPList() {
        return this.m_remoteEPList;
    }

    get lastSendTime() {
        return this.m_lastSendTime;
    }

    updateActiveEP(ep) {
        this.m_activeEP = ep;
        if (ep) {
            this.addRemoteEPList([ep]);
        }
    }

    get activeEP() {
        return this.m_activeEP;
    }

    addRemoteEPList(addEPList) {
        let epSet = new Set(this.m_remoteEPList);
        let count = epSet.size;
        let newEPList = [];
        addEPList.forEach(ep => {
            epSet.add(ep);
            if (epSet.size !== count) {
                count = epSet.size;
                newEPList.push(ep);
            }
        });
        if (epSet.size !== this.m_remoteEPList.length) {
            this.m_remoteEPList = [...epSet];
        }
        return newEPList;
    }

    get isResend() {
        return this.m_isResend;
    }

    set isResend(enable) {
        this.m_isResend = enable;
    }

    abort() {
        this.m_abort = true;
    }

    equal(other) {
        if (!other) {
            return false;
        }
        if (!(other instanceof BDTPackageSender)) {
            return false;
        }
        if (other.m_socket !== this.m_socket) {
            return false;
        }
        if (other.m_mixSocket !== this.m_mixSocket) {
            return false;
        }

        for (let otherEP of other.m_remoteEPList) {
            let found = false;
            for (let myEP of this.m_remoteEPList) {
                if (otherEP === myEP) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return false;
            }
        }
        return other.m_remoteEPList.length === this.m_remoteEPList.length;
    }
}


module.exports.BDTPackage = BDTPackage;
module.exports.BDT_ERROR = BDT_ERROR;
module.exports.BDTPackageSender = BDTPackageSender;
