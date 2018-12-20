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
const {Result: DHTResult, Config} = require('../util.js');
const Task = require('./task.js');
const DHTPackageFactory = require('../package_factory.js');
const DHTPackage = require('../packages/package.js');
const DHTCommandType = DHTPackage.CommandType;
const {ResendControlor} = require('../package_sender.js');
const {Peer, LocalPeer} = require('../peer.js');
const BaseUtil = require('../../base/util.js');
const TimeHelper = BaseUtil.TimeHelper;

const TaskConfig = Config.Task;
const PackageConfig = Config.Package;

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

class SplitPackageTask extends Task {
    constructor(owner, cmdPackage, peer) {
        super(owner, {timeout: PackageConfig.Timeout});
        this.m_cmdPackage = cmdPackage;
        this.m_peer = peer;
        this.m_address = null;
        this.m_sendingPieces = new Map();
        this.m_owner = owner;
    }

    _startImpl() {
        LOG_DEBUG(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) start SplitPackage(seq:${this.m_cmdPackage.common.seq},type:${this.m_cmdPackage.cmdType},taskid:${this.id}) to ${this.m_sendingPieces.length} pieces.`);
        this._genPieces();
        for (let [no, pkg] of this.m_sendingPieces) {
            pkg.resender.send();
        }
    }
    
    _stopImpl() {
    }

    _processImpl(response, remotePeer) {
        LOG_DEBUG(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) SplitPackage (seq:${this.m_cmdPackage.common.seq},type:${this.m_cmdPackage.cmdType},taskid:${this.id}) response (${response.common.ackSeq}:${response.body.taskid}:${response.body.no}).`);
        if (!response.body.no && response.body.no !== 0) {
            return;
        }
        
        let no = parseInt(response.body.no);
        let arrivedPkg = this.m_sendingPieces.get(no);
        if (arrivedPkg) {
            this.m_sendingPieces.delete(no);
            arrivedPkg.resender.finish();
        }

        if (this.m_sendingPieces.size === 0) {
            LOG_DEBUG(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) SplitPackage (seq:${this.m_cmdPackage.common.seq},type:${this.m_cmdPackage.cmdType},taskid:${this.id}) done.`);
            this._onComplete(DHTResult.SUCCESS);
        }
    }

    _retryImpl() {
        this.m_sendingPieces.forEach(pkg => pkg.resender.send());
    }

    _onCompleteImpl(result) {
        this.m_sendingPieces.forEach(pkg => pkg.resender.finish());
    }

    _genPieces() {
        if (this.m_cmdPackage.cmdPackage === DHTCommandType.PACKAGE_PIECE_REQ) {
            this.m_owner.BODY_LIMIT = 0;
        }

        let encoder = DHTPackageFactory.createEncoder(this.m_cmdPackage);
        let sendingBuffer = encoder.encode();
        const bodyLimit = this._bodyLimit();
        let pieceCount = Math.ceil(sendingBuffer.length / bodyLimit);
        let maxPkgNo = pieceCount - 1;

        let bodyOffset = 0;

        for (let pkgNo = 0; pkgNo < pieceCount; pkgNo++) {
            let piecePkg = this.packageFactory.createPackage(DHTCommandType.PACKAGE_PIECE_REQ);
            piecePkg.body = {
                taskid: this.id,
                peerid: this.bucket.localPeer.peerid,
                max: maxPkgNo,
                no: pkgNo,
                buf: sendingBuffer.slice(bodyLimit * pkgNo, Math.min(sendingBuffer.length, bodyLimit * (pkgNo + 1))),
            }

            piecePkg.__orignalCmdType = this.m_cmdPackage.cmdType;
            piecePkg.resender = new ResendControlor(this.m_peer,
                piecePkg,
                this.packageSender,
                Peer.retryInterval(this.bucket.localPeer, this.m_peer),
                Config.Package.RetryTimes,
                true);
            this.m_sendingPieces.set(pkgNo, piecePkg);
        }
    }

    _bodyLimit() {
        const now = TimeHelper.uptimeMS();
        const localPeer = this.bucket.localPeer;
        if (!this.m_owner.BODY_LIMIT || now - this.m_owner.BODY_LIMIT_CALC_TIME > 600809) {
            let emptyPiecePkg = this.packageFactory.createPackage(DHTCommandType.PACKAGE_PIECE_REQ);
            emptyPiecePkg.body = {
                taskid: TaskConfig.MaxTaskID,
                max: 0xFFFFFFFF,
                no: 0xFFFFFFFF,
                buf: Buffer.allocUnsafe(0),
                sz: 0,
            }

            let peerStruct = localPeer.toStructForPackage();

            if (!this.m_peer.hash) {
                this.m_peer.hash = HashDistance.hash(this.m_peer.peerid);
            }
            let destInfo = {
                peerid: this.m_peer.peerid,
                hash: this.m_peer.hash,
                ep: '',
            };
            emptyPiecePkg.fillCommon(peerStruct, destInfo);

            let encoder = DHTPackageFactory.createEncoder(emptyPiecePkg);
            let pkgBuffer = encoder.encode();

            const MAX_EP_STRING_LENGTH = 46 + 7; // @IPV6@PORT@u
            let emptyPiecePkgLength = pkgBuffer.length + MAX_EP_STRING_LENGTH;
            this.m_owner.BODY_LIMIT = DHTPackageFactory.PACKAGE_LIMIT - emptyPiecePkgLength - 4; // 4是sz长度，编码后会把sz设定为分片的真实长度
            this.m_owner.BODY_LIMIT_CALC_TIME = now;
        }
        return this.m_owner.BODY_LIMIT - localPeer.peerid.length;
    }
}

module.exports = SplitPackageTask;