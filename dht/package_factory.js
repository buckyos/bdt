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

const assert = require('assert');
const msgpack = require('msgpack-lite');
const Base = require('../base/base.js');
const {Config, Result: DHTResult, SequenceIncreaseGenerator} = require('./util.js');
const DHTPackage = require('./packages/package.js');
const CommandType = DHTPackage.CommandType;

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

const PackageConfig = Config.Package;

const PROTOCOL_VERSION = 0x101;
const UDP_MTU = 1450;
const PACKAGE_LIMIT = UDP_MTU;
const MAGIC_NUM = PackageConfig.MagicNum;

const ZERO_BUFFER = Buffer.allocUnsafe(0);

class PackageEncoder {
    constructor(pkg) {
        this.m_package = pkg;
        this.m_buffer = null;
        this.m_commonBuffer = null;
        this.m_bodyBuffer = null;
    }

    encode(buffer = null, offset = 0, size = PACKAGE_LIMIT) {
        this.m_package.checkAllField();
        if (buffer) {
            if (size < this.needBufferSize()) {
                return null;
            }
            this.m_buffer = buffer.slice(offset, offset + size);
        } else {
            this.m_buffer = Buffer.allocUnsafe(this.needBufferSize());
        }

        let commonBuffer = this.encodeCommon();
        let bodyBuffer = this.encodeBody();
        offset = DHTPackage.HEADER_LENGTH;
        bodyBuffer.copy(this.m_buffer, offset);
        offset += bodyBuffer.length;
        if (this.m_package.cmdType === CommandType.PACKAGE_PIECE_REQ) {
            this.m_package.body.buf.copy(this.m_buffer, offset);
            offset += this.m_package.body.buf.length;
        } else {
            commonBuffer.copy(this.m_buffer, offset);
            offset += commonBuffer.length;
        }
        this.encodeHeader(this.m_buffer, 0);
        return this.m_buffer;
    }

    needBufferSize() {
        let bodyLength = this.encodeBody().length;
        if (this.m_package.cmdType === CommandType.PACKAGE_PIECE_REQ) {
            bodyLength += this.m_package.body.buf.length;
        }
        let commonLimit = PACKAGE_LIMIT - bodyLength - DHTPackage.HEADER_LENGTH;
        let nodeCount = this.m_package.nodes? this.m_package.nodes.length : 0;
        let commonLength = this.encodeCommon(nodeCount).length;
        while (commonLength > commonLimit && nodeCount > 0) {
            this.m_commonBuffer = null;
            nodeCount--;
            commonLength = this.encodeCommon(nodeCount).length;
        }
        return DHTPackage.HEADER_LENGTH + bodyLength + commonLength;
    }

    encodeHeader(buffer, offset) {
        let pkgCommon = this.m_package.common;
        let startOffset = offset;

        let totalLength = DHTPackage.HEADER_LENGTH;
        totalLength += this.m_commonBuffer? this.m_commonBuffer.length : 0;
        totalLength += this.m_bodyBuffer? this.m_bodyBuffer.length : 0;
        if (this.m_package.cmdType === CommandType.PACKAGE_PIECE_REQ) {
            totalLength += this.m_package.body.buf.length;
        }
        offset = buffer.writeUInt16LE(MAGIC_NUM, offset);
        offset = buffer.writeUInt16LE(PROTOCOL_VERSION, offset);
        offset = buffer.writeUInt16LE(pkgCommon.cmdType, offset);
        offset = buffer.writeUInt16LE(totalLength, offset);
        offset = buffer.writeUInt32LE(pkgCommon.appid, offset);
        offset = buffer.writeUInt16LE(this.m_bodyBuffer? this.m_bodyBuffer.length : 0, offset);
        offset = buffer.writeUInt32LE(pkgCommon.src.hash, offset);
        offset = buffer.writeUInt32LE(pkgCommon.src.onlineDuration, offset);
        offset = buffer.writeUInt8(pkgCommon.src.natType, offset);
        offset = buffer.writeUInt32LE(pkgCommon.dest.hash, offset);
        offset = buffer.writeUInt32LE(pkgCommon.seq, offset);
        offset = buffer.writeUInt32LE(pkgCommon.ackSeq, offset);
        offset = buffer.writeUInt8(pkgCommon.ttl, offset);
        assert(offset - startOffset === DHTPackage.HEADER_LENGTH,
            `PackageHeader encode failed. type:${CommandType.toString(this.m_cmdType)}, headersize:${offset - startOffset}`);
    }

    encodeCommon(nodeCount) {
        if (!this.m_commonBuffer) {
            if (this.m_package.cmdType === CommandType.PACKAGE_PIECE_REQ) {
                this.m_commonBuffer = ZERO_BUFFER;
                return this.m_commonBuffer;
            }
            let pkgCommon = this.m_package.common;
            let src = pkgCommon.src;
            let dest = pkgCommon.dest;
            let nodes = pkgCommon.nodes;
            
            let commonObj = {
                src: {
                    id: src.peerid,
                    // eplist: src.eplist,
                    // services: src.services
                    // info: src.additionalInfo
                },
                dest: {
                    id: dest.peerid,
                    //ep: dest.ep,
                },
                // nodes: 非必须
                // id: 包标识
            }

            if (src.eplist) {
                commonObj.src.eplist = src.eplist;
            }
            if (src.services) {
                commonObj.src.services = src.services;
            }
            if (src.additionalInfo) {
                commonObj.src.info = src.additionalInfo;
            }
            if (src.inactive) {
                commonObj.src.inactive = true;
            }
            if (dest.ep) {
                commonObj.dest.ep = dest.ep;
            }
            if (pkgCommon.packageID) {
                commonObj.id = pkgCommon.packageID;
            }

            // 随机选取nodeCount个节点信息发送出去
            if (nodes && nodes.length > 0) {
                if (typeof nodeCount !== 'number') {
                    nodeCount = nodes.length;
                }

                commonObj.nodes = [];
                let startIndex = Math.round(Math.random() * (nodes.length - 1));
                for (let i = startIndex; i < nodes.length && i < startIndex + nodeCount; i++) {
                    commonObj.nodes.push(nodes[i]);
                }
                for (let i = 0; i < startIndex && i < nodeCount - (nodes.length - startIndex); i++) {
                    commonObj.nodes.push(nodes[i]);
                }
            }
            this.m_commonBuffer = msgpack.encode(commonObj);
        }
        return this.m_commonBuffer;
    }

    encodeBody() {
        if (!this.m_bodyBuffer) {
            if (this.m_package.body) {
                if (this.m_package.cmdType === CommandType.PACKAGE_PIECE_REQ) {
                    let piece = this.m_package.body.buf;
                    delete this.m_package.body.buf;
                    this.m_package.body.sz = piece.length;
                    this.m_bodyBuffer = msgpack.encode(this.m_package.body);
                    this.m_package.body.buf = piece;
                } else {
                    this.m_bodyBuffer = msgpack.encode(this.m_package.body);
                }
            } else {
                this.m_bodyBuffer = ZERO_BUFFER;
            }
        }
        return this.m_bodyBuffer;
    }
}

class PackageDecoder {
    constructor(buffer) {
        this.m_buffer = buffer;
        this.m_package = null;
        this.m_headerLength = 0;
        this.m_bodyLength = 0;
        this.m_totalLength = 0;
    }

    decode() {
        if (this.decodeHeader() !== DHTResult.SUCCESS) {
            return null;
        }
        if (this.m_totalLength > this.m_buffer.length) {
            return null;
        }

        if (!this.decodeBody()) {
            return null;
        }
        if (this.m_package.cmdType !== CommandType.PACKAGE_PIECE_REQ) {
            if (!this.decodeCommon()) {
                return null;
            }
        }
        return this.m_package;
    }

    get totalLength() {
        return this.m_totalLength;
    }
    
    get package() {
        return this.m_package;
    }

    decodeHeader() {
        if (this.m_package) {
            return DHTResult.SUCCESS;
        }

        if (this.m_buffer.length < DHTPackage.HEADER_LENGTH) {
            return DHTResult.INVALID_PACKAGE;
        }
        let offset = 0;
        let magic = this.m_buffer.readUInt16LE(offset);
        // assert(magic === MAGIC_NUM, `Decoder got incorrect package. magic = ${magic}`);
        offset += 2;
        let version = this.m_buffer.readUInt16LE(offset);
        offset += 2; // version
        if (magic !== MAGIC_NUM || version !== PROTOCOL_VERSION) {
            return DHTResult.INVALID_PACKAGE;
        }
        this.m_headerLength = DHTPackage.HEADER_LENGTH;
        let cmdType = this.m_buffer.readUInt16LE(offset);
        offset += 2;
        this.m_totalLength = this.m_buffer.readUInt16LE(offset);
        offset += 2;
        let appid = this.m_buffer.readUInt32LE(offset);
        offset += 4;
        this.m_bodyLength = this.m_buffer.readUInt16LE(offset);
        offset += 2;
        this.m_package = new DHTPackage(cmdType);
        let pkgCommon = this.m_package.common;
        pkgCommon.appid = appid;
        pkgCommon.src.hash = this.m_buffer.readUInt32LE(offset);
        offset += 4;
        pkgCommon.src.onlineDuration = this.m_buffer.readUInt32LE(offset);
        offset += 4;
        pkgCommon.src.natType = this.m_buffer.readUInt8(offset);
        offset += 1;
        pkgCommon.dest.hash = this.m_buffer.readUInt32LE(offset);
        offset += 4;
        pkgCommon.seq = this.m_buffer.readUInt32LE(offset);
        offset += 4;
        pkgCommon.ackSeq = this.m_buffer.readUInt32LE(offset);
        offset += 4;
        pkgCommon.ttl = this.m_buffer.readUInt8(offset);
        offset += 1;
        if (pkgCommon.ttl > PackageConfig.MaxTTL) {
            pkgCommon.ttl = PackageConfig.MaxTTL;
        }
        if (this.m_totalLength < DHTPackage.HEADER_LENGTH + this.m_bodyLength) {
            return DHTResult.INVALID_PACKAGE;
        }
        return DHTResult.SUCCESS;
    }

    decodeCommon() {
        if (!this.m_package) {
            if (decodeHeader() !== DHTResult.SUCCESS) {
                return null;
            }
        }

        if (this.m_totalLength > this.m_buffer.length) {
            return null;
        }
        let pkgCommon = this.m_package.common;
        if (pkgCommon.src.peerid) {
            return pkgCommon;
        }

        let commonOffset = this.m_headerLength + this.m_bodyLength;
        if (commonOffset >= this.m_buffer.length) {
            return null;
        }/* else {
            assert(commonOffset < this.m_buffer.length,
                `Decoder got package that lost the COMMON field. cmdType:${this.m_package.cmdType}, size(header/body/total) = ${this.m_headerLength}/${this.m_bodyLength}/${this.m_buffer.length}`);
        }*/
        let commonBuffer = this.m_buffer.slice(commonOffset, this.m_buffer.length);
        let commonObj = null;
        try {
            commonObj = msgpack.decode(commonBuffer);
            assert(commonObj.src && commonObj.src.id && commonObj.dest && commonObj.dest.id && commonObj.dest.ep,
                `common error:${JSON.stringify(commonObj)},cmdType:${this.m_package.cmdType}`);
        } catch (error) {
            return null;
        }
        pkgCommon.src.peerid = commonObj.src.id;
        pkgCommon.src.eplist = commonObj.src.eplist || [];
        pkgCommon.dest.peerid = commonObj.dest.id;
        pkgCommon.dest.ep = commonObj.dest.ep;

        if ('info' in commonObj.src) {
            pkgCommon.src.additionalInfo = commonObj.src.info;
        }
        if ('services' in commonObj.src) {
            pkgCommon.src.services = commonObj.src.services;
        }
        if ('inactive' in commonObj.src) {
            pkgCommon.src.inactive = commonObj.src.inactive;
        }
        if ('nodes' in commonObj) {
            pkgCommon.nodes = commonObj.nodes;
        }
        if ('id' in commonObj) {
            pkgCommon.packageID = commonObj.id;
        }

        this.m_package.checkCommon();
        return pkgCommon;
    }

    decodeBody() {
        if (!this.m_package) {
            if (decodeHeader() !== DHTResult.SUCCESS) {
                return null;
            }
        }

        if (this.m_package.body) {
            return this.m_package.body;
        }

        if (this.m_totalLength > this.m_buffer.length) {
            return null;
        }

        let bodyBuffer = this.m_buffer.slice(this.m_headerLength, this.m_headerLength + this.m_bodyLength);
        if (!this.m_package.decodeBody(bodyBuffer)) {
            return null;
        }

        if (this.m_package.cmdType === CommandType.PACKAGE_PIECE_REQ) {
            if (typeof this.m_package.body.sz !== 'object' &&
                (this.m_package.body.sz || this.m_package.body.sz === 0)) {
                this.m_package.body.sz = parseInt(this.m_package.body.sz);
                this.m_package.body.buf = this.m_buffer.slice(this.m_headerLength + this.m_bodyLength,
                    this.m_headerLength + this.m_bodyLength + this.m_package.body.sz);
                this.m_bodyLength += this.m_package.body.sz;
            }
        }
        this.m_package.checkBody();
        return this.m_package.body;
    }
}

class PackageFactory {
    constructor(appid) {
        this.m_appid = appid;
        this.m_seqGen = new SequenceIncreaseGenerator(PackageConfig.MinSeq, PackageConfig.MaxSeq);
    }

    get appid() {
        return this.m_appid;
    }

    createPackage(cmdType) {
        return new DHTPackage(cmdType, this.m_seqGen.genSeq(), this.m_appid);
    }

    static createEncoder(dhtPackage) {
        return new PackageEncoder(dhtPackage);
    }

    static createDecoder(buffer, offset = 0, length = Infinity) {
        if (!isFinite(length)) {
            length = buffer.length - offset;
        }

        if (length < DHTPackage.HEADER_LENGTH) {
            // assert(false, `package too short(${length})`);
            return null;
        }

        let magic = buffer.readUInt16LE(offset);
        if (magic != MAGIC_NUM) {
            // assert(false, `package magicnum error(${magic})`);
            return null;
        }

        return new PackageDecoder(buffer.slice(offset, offset + length));
    }
}

PackageFactory.PACKAGE_LIMIT = PACKAGE_LIMIT;
PackageFactory.HEADER_LENGTH = DHTPackage.HEADER_LENGTH;

module.exports = PackageFactory;