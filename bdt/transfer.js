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
const baseModule = require('../base/base');
const BaseUtil = require('../base/util');
const blog = baseModule.blog;
const packageModule = require('./package');
const BDTPackage = packageModule.BDTPackage;
const BDT_ERROR = packageModule.BDT_ERROR;
const BDTSendBuffer = require('./send_buffer');
const assert = require('assert');
const SequenceU32 = BaseUtil.SequenceU32;
const TimeHelper = BaseUtil.TimeHelper;

const _DEBUG = false;

let md5 = null;
if (_DEBUG) {
    md5 = buffer => {
        const Crypto = require('crypto');
        let md5 = Crypto.createHash('md5');
        md5.update(buffer);
        let md5Hash = md5.digest();
        return md5Hash.readUInt32BE(0);
    }
}

const AckType = {
    ack: 1,
    data: 2,
    finalAck: 3,
};

class BDTSendQueue {
    constructor(transfer) {
        this.m_transfer = transfer;
        this.m_connection = transfer.connection;
        this.m_ackSeq = this.sentSeq;
        this.m_pending = [];
        this.m_waitResend = [];
        this.m_waitAck = [];
        this.m_maxAckSeq = this.m_ackSeq;

        this.m_dumpAckCounter = 0;
    }

    get sentSeq() {
        return SequenceU32.sub(this.m_connection._nextSeq(0), 1);
    }

    get ackSeq() {
        return this.m_ackSeq;
    }

    get flightSize() {
        if (this.m_waitAck.length > 0) {
            return SequenceU32.delta(this.maxWaitAckSeq, this.m_ackSeq);
        }
        return 0;
    }

    get dumpCount() {
        return this.m_dumpAckCounter;
    }

    get maxWaitAckSeq() {
        return SequenceU32.sub(this.m_waitAck[this.m_waitAck.length - 1].package.nextSeq, 1);
    }

    allocDataPackage(buffers, fin=false) {
        assert(this.m_waitResend.length === 0 || fin, `waitResend:${this.m_waitResend.length},waitAck:${this.m_waitAck.length}`);
        let encoder = null;
        if (!fin) {
            encoder = this.m_connection._createPackageHeader(BDTPackage.CMD_TYPE.data);
            encoder.addData(buffers);
        } else {
            encoder = this.m_connection._createPackageHeader(BDTPackage.CMD_TYPE.fin);
        }
        encoder.header.seq = this.m_connection._nextSeq(encoder.dataLength + 1);
        let stub = {
            sentTime: TimeHelper.uptimeMS(),
            limitAckSeq: encoder.nextSeq, // 要在limitAckSeq被确认之前ack，否则应当择机重传
            sackCount: 0, // 该包后续分包被ack的数量
            package: encoder,
        };
        this.m_pending.push(stub);
        if (this.m_waitResend.length > 0) {
            this.m_waitResend.push(stub);
            stub.sentTime = 0;
            return null;
        } else {
            this.m_waitAck.push(stub);
        }
        return encoder;
    }

    onAckPackage(ackSeq, ackType) {
        assert(this.m_waitAck.length === 0 || SequenceU32.compare(this.m_waitAck[0].package.header.seq, SequenceU32.add(this.m_ackSeq, 1)) >= 0, 
            `waitAck.seq:${this.m_waitAck.length? this.m_waitAck[0].package.header.seq : 0},ackSeq:${this.m_ackSeq}`);
        assert(this.m_waitResend.length === 0 || SequenceU32.compare(this.m_waitResend[0].package.header.seq, this.m_waitAck[0].package.header.seq) > 0, 
            `waitAck.seq:${this.m_waitAck.length? this.m_waitAck[0].package.header.seq : 0},waitResend.seq:${this.m_waitResend.length? this.m_waitResend[0].package.header.seq : 0}`);
        if (SequenceU32.compare(ackSeq, this.m_ackSeq) > 0) {
            // 比上一个收到的小时 不更新cwnd
            this.m_dumpAckCounter = 1;
            let stubIndex = null;
            for (let index = 0; index < this.m_pending.length; ++index) {
                let stub = this.m_pending[index];
                if (SequenceU32.compare(stub.package.ackSeq, ackSeq) === 0) {
                    stubIndex = index;
                    break;
                } else if (SequenceU32.compare(ackSeq, stub.package.ackSeq) < 0) {
                    break;
                }
            }
            if (stubIndex == null) {
                // 不存在这个seq
                return [0, null];
            }
            let acked = SequenceU32.delta(ackSeq, this.m_ackSeq);
            this.m_ackSeq = ackSeq;
            let stubs = this.m_pending.splice(0, stubIndex + 1);
            if (SequenceU32.compare(ackSeq, this.m_maxAckSeq) > 0) {
                this.m_maxAckSeq = ackSeq;
            }

            let removeFromUnackedQueue = unackedQueue => {
                if (unackedQueue.length === 0) {
                    return;
                }
                if (SequenceU32.compare(unackedQueue[unackedQueue.length - 1].package.ackSeq, ackSeq) <= 0) {
                    unackedQueue.splice(0, unackedQueue.length);
                    return;
                }
                for (let i = 0; i < unackedQueue.length; i++) {
                    if (SequenceU32.compare(unackedQueue[i].package.ackSeq, ackSeq) > 0) {
                        unackedQueue.splice(0, i);
                        break;
                    } else if (SequenceU32.compare(unackedQueue[i].package.ackSeq, ackSeq) === 0) {
                        unackedQueue.splice(0, i + 1);
                        break;
                    }
                }
            }

            removeFromUnackedQueue(this.m_waitAck);
            removeFromUnackedQueue(this.m_waitResend);
            return [acked, stubs[stubs.length - 1]];
        } else if (SequenceU32.compare(ackSeq, this.m_ackSeq) === 0) {
            if (this.m_pending.length) {
                // 连续收到3个相同的专用ack包，进入快速重传状态，这里需要计数
                if (ackType === AckType.ack) {
                    ++this.m_dumpAckCounter;
                    return [0, this.m_pending[0]];
                } else if (ackType === AckType.finalAck) {
                    // 重发ack只要对第一个包重传一下好了
                    return [0, this.m_pending[0]];
                }
                return [-1, null];
            } else {
                this.m_dumpAckCounter = 1;
                return [-1, null];
            }
        } else {
            return [-1, null];
        }
    }

    onSACK(sack) {
        let _doSACK = unackedQueue => {
            let offset = 0;
            let fromSeq = 0;
            let toSeq = 0;
            let sendPkgIndex = 0;
            // 接收端返回的sack是从小到大顺序排列的
            while (true) {
                if (sack.length - offset < 8 || sendPkgIndex >= unackedQueue.length) {
                    return;
                }
                fromSeq = sack.readUInt32LE(offset);
                toSeq = sack.readUInt32LE(offset + 4);
                offset += 8;

                let toAckSeq = SequenceU32.sub(toSeq, 1);
                if (SequenceU32.compare(toAckSeq, this.m_maxAckSeq) > 0) {
                    this.m_maxAckSeq = toAckSeq;
                }

                let ackCount = 0;
                while (sendPkgIndex < unackedQueue.length) {
                    let stub = unackedQueue[sendPkgIndex];
                    // toSeq本身不被ack
                    if (SequenceU32.compare(stub.package.header.seq, fromSeq) >= 0 &&
                        SequenceU32.compare(stub.package.header.seq, toSeq) < 0) {
                        unackedQueue.splice(sendPkgIndex, 1);
                        stub.sack = true;
                        ackCount++;
                    } else {
                        sendPkgIndex++;
                    }
                }

                // 更新乱序ack的数量
                for (let i = 0; i < sendPkgIndex; i++) {
                    let stub = unackedQueue[i];
                    if (SequenceU32.compare(stub.limitAckSeq, toAckSeq) < 0) {
                        stub.sackCount += ackCount;
                    }
                }
            }
        }
        _doSACK(this.m_waitAck);
        _doSACK(this.m_waitResend);
    }

    resendWaitAck(timeout, onResent) {
        let now = TimeHelper.uptimeMS();
        let count = 0;
        let index = 0;
        let isTimeout = () => {
            for (; index < this.m_waitAck.length; index++) {
                let stub = this.m_waitAck[index];
                assert(stub.sentTime > 0, `sentTime:${stub.sentTime}`);
                let diff = now - stub.sentTime;
                if (diff > timeout) {
                    return true;
                }
            }
            return false;
        }

        if (!isTimeout()) {
            return 0;
        }

        // 一个包发生超时，重建整个发送窗口
        let rtoMin = this.m_connection.stack._getOptions().rtoMin;
        for (index = 0; index < this.m_waitAck.length; index++) {
            let stub = this.m_waitAck[index];
            assert(stub.sentTime > 0, `sentTime:${stub.sentTime}`);
            
            let diff = now - stub.sentTime;
            if (diff > rtoMin) {
                stub.lastSentTime = stub.sentTime;
                stub.sentTime = now;
                stub.limitAckSeq = stub.package.nextSeq;
                stub.sackCount = 0;
                stub.package.header.isResend = true;
                this.m_transfer._postPackage(stub.package);
                ++count;
                if (!onResent(stub)) {
                    break;
                }
            }
        }

        let firstResendIndex = index + 1;
        if (firstResendIndex < this.m_waitAck.length) {
            let newResend = this.m_waitAck.splice(firstResendIndex, this.m_waitAck.length - firstResendIndex);
            if (this.m_waitResend.length === 0) {
                this.m_waitResend = newResend;
            } else if (SequenceU32.compare(newResend[newResend.length - 1].package.ackSeq, this.m_waitResend[0].package.ackSeq) < 0){
                this.m_waitResend.unshift(...newResend);
            } else {
                this.m_waitResend.push(...newResend);
            }
        }
        return count;
    }

    resendWaitResend(onResent) {
        if (this.m_waitResend.length === 0) {
            return 0;
        }

        let now = TimeHelper.uptimeMS();
        let lastResendIndex = 0;
        for (lastResendIndex = 0; lastResendIndex < this.m_waitResend.length; lastResendIndex++) {
            let stub = this.m_waitResend[lastResendIndex];
            stub.lastSentTime = stub.sentTime;
            stub.sentTime = now;
            stub.limitAckSeq = stub.package.nextSeq;
            stub.sackCount = 0;
            stub.package.header.isResend = stub.lastSentTime > 0;
            this.m_transfer._postPackage(stub.package);
            if (onResent(stub)) {
                break;
            }
        }

        let resentPkgs = null;
        if (lastResendIndex < this.m_waitResend.length) {
            resentPkgs = this.m_waitResend.splice(0, lastResendIndex + 1);
        } else {
            resentPkgs = this.m_waitResend;
            this.m_waitResend = [];
        }
        if (resentPkgs.length > 0) {
            this.m_waitAck.push(...resentPkgs);
        }
        return resentPkgs.length;
    }

    resendNext(forceFirst) {
        let _send = stub => {
            stub.package.header.isResend = true;
            stub.lastSentTime = stub.sentTime;
            stub.sentTime = TimeHelper.uptimeMS();
            stub.limitAckSeq = SequenceU32.add(this.maxWaitAckSeq, 1);
            stub.sackCount = 0;
            this.m_transfer._postPackage(stub.package);
        }

        if (this.m_waitAck.length === 0) {
            return 0;
        }

        if (forceFirst) {
            _send(this.m_waitAck[0]);
            return 1; 
        }

        let sentCount = 0;
        for (let stub of this.m_waitAck) {
            if (stub.sackCount < 3) {
                break;
            }
            if (SequenceU32.compare(stub.limitAckSeq, this.m_maxAckSeq) < 0 && stub.sackCount === 3) {
                _send(stub);
                sentCount++;
            }
        }
        return sentCount;
    }
}

class BDTRecvQueue {
    constructor(connection) {
        const opt = connection.stack._getOptions();
        this.m_connection = connection;
        this.m_pending = [];
        this.m_lastAckSeq = 0;
        this.m_quickAckCount = opt.quickAckCount;
        this.m_lastRecvTime = TimeHelper.uptimeMS();
        this.m_ato = opt.ackTimeoutMax;
    }

    get nextSeq() {
        return this.m_connection._getNextRemoteSeq();
    }

    get ackSeq() {
        return SequenceU32.sub(this.nextSeq, 1);
    }

    get waitAckSize() {
        return SequenceU32.delta(this.m_connection._getNextRemoteSeq(), this.m_lastAckSeq);
    }

    get isQuickAck() {
        let opt = this.m_connection.stack._getOptions();
        return this.m_quickAckCount > 0 ||this.m_pending.length > 0 || this.waitAckSize >= opt.ackSize;
    }

    get ato() {
        return this.m_ato;    
    }

    allocAckPackage(windowSize) {
        let encoder = this.m_connection._createPackageHeader(BDTPackage.CMD_TYPE.data);
        this.fillAckPackage(encoder, windowSize);
        if (this.m_pending.length > 0) {
            let sackBuffer = Buffer.allocUnsafe(this.m_pending.length * 8);
            let offset = 0;
            this.m_pending.forEach(stub => {
                // sack区间[seq, nextSeq)，被ack的范围不包括nextSeq
                sackBuffer.writeUInt32LE(stub.seq, offset);
                sackBuffer.writeUInt32LE(stub.nextSeq, offset + 4);
                offset += 8;
            });
            encoder.addData([sackBuffer]);
            encoder.header.sack = true;
        }
        return encoder;
    }

    fillAckPackage(encoder, windowSize) {
        encoder.header.ackSeq = this.ackSeq;
        encoder.header.windowSize = windowSize;
        this.m_lastAckSeq = encoder.header.ackSeq;
        if (this.m_quickAckCount > 0) {
            this.m_quickAckCount--;
        }
        return encoder;
    }

    addPackage(decoder) {
        let header = decoder.header;
        let pending = this.m_pending;
        this._updateAto();

        let assertSeq = (queue, beginSeq) => {
            if (queue.length === 0) {
                return;
            }
            let nextSeq = beginSeq || queue[0].header.seq;
            queue.forEach(pkg => {
                assert(SequenceU32.compare(pkg.header.seq, nextSeq) === 0, `nextSeq:${nextSeq},pkg.seq:${pkg.header.seq}`);
                nextSeq = pkg.nextSeq;
            });
        }

        if (SequenceU32.compare(header.seq, this.nextSeq) === 0) {
            let unpend = null; 
            if (pending.length) {
                let stub = pending[0];
                if (SequenceU32.compare(stub.seq, decoder.nextSeq) === 0) {
                    pending.splice(0, 1);
                    stub.packages.unshift(decoder);
                    unpend = stub.packages;
                } else {
                    unpend = [decoder];
                }
            } else {
                unpend = [decoder];
            }

            assertSeq(unpend, this.nextSeq);
            this.m_connection._setNextRemoteSeq(unpend[unpend.length - 1].nextSeq);
            return unpend;
        } else if (SequenceU32.compare(header.seq, this.nextSeq) > 0) {
            let isCached = false;
            for (let index = 0; index < pending.length; ++index) {
                let stub = pending[index];
                if (SequenceU32.compare(stub.seq, decoder.nextSeq) > 0) {
                    pending.splice(index, 0, {
                        seq: header.seq,
                        nextSeq: decoder.nextSeq,
                        packages: [decoder]
                    });
                    isCached = true;
                    break;    
                }
                
                if (SequenceU32.compare(stub.seq, decoder.nextSeq) === 0) {
                    stub.seq = header.seq;
                    stub.packages.unshift(decoder);
                    isCached = true;
                    assertSeq(stub.packages);
                    if (index > 0) {
                        let preStub = pending[index - 1];
                        if (SequenceU32.compare(preStub.nextSeq, stub.seq) === 0) {
                            // 应该不能运行到这里，因为在前序序列的时候就会被处理
                            assert(false, `cannot reach here.`);
                            preStub.nextSeq = stub.nextSeq;
                            preStub.packages = preStub.packages.concat(stub.packages);
                            pending.splice(index, 1);
                            assertSeq(preStub.packages);
                        }
                    }
                    break;
                }

                if (SequenceU32.compare(stub.nextSeq, header.seq) === 0) {
                    stub.nextSeq = decoder.nextSeq;
                    stub.packages.push(decoder);
                    isCached = true;
                    assertSeq(stub.packages);
                    if (index < pending.length - 1) {
                        let nextStub = pending[index + 1];
                        const d = SequenceU32.compare(stub.nextSeq, nextStub.seq);
                        if (d === 0) {
                            stub.nextSeq = nextStub.nextSeq;
                            stub.packages = stub.packages.concat(nextStub.packages);
                            pending.splice(index + 1, 1);
                            assertSeq(stub.packages);
                        } else if (d > 0) {
                            // seq有重叠，可能有恶意节点冒名发送的包
                            assert(false, `${d}`);
                            pending.splice(index + 1, 1);
                        }
                    }
                    break;
                }

                if (SequenceU32.compare(header.seq, stub.seq) >= 0 && SequenceU32.compare(decoder.nextSeq, stub.nextSeq) <= 0) {
                    isCached = true;
                    break;
                }
            }

            if (!isCached) {
                pending.push({
                    seq: header.seq,
                    nextSeq: decoder.nextSeq,
                    packages: [decoder]
                });
            }
        }

        return null;
    }

    _updateAto() {
        let now = TimeHelper.uptimeMS();
        let lastRecvTime = this.m_lastRecvTime;
        const opt = this.m_connection.stack._getOptions();
        const atoMin = opt.ackTimeoutMin;
        const atoMax = opt.ackTimeoutMax;

        this.m_lastRecvTime = now;

        const delta = now - lastRecvTime;
        const halfATOMin = atoMin / 2;

        if (delta <= halfATOMin) {
            this.m_ato = this.m_ato / 2 + halfATOMin;
        } else if (delta <= this.m_ato) {
            this.m_ato = Math.min(this.m_ato / 2 + delta, atoMax);
        } else {
            // 比ack周期还长，可能是丢包或者是发送窗口满，或者进入慢启动等
            this.m_quickAckCount = opt.quickAckCount;
        }
    }
}

class Reno {
    constructor(connection) {
        this.m_connection = connection;
        const opt = connection.stack._getOptions();
        this.m_mms = opt.udpMMS;
        this.m_cwnd = opt.udpMMS;
        this.m_rtoMin = opt.rtoMin;
        this.m_rtoMax = opt.rtoMax;
        
        this.m_ssthresh = opt.initRecvWindowSize;

        this.m_srtt = 0;
        this.m_rttvar = opt.initRTTVar;
        this.m_rto = this.m_srtt + 4*this.m_rttvar;

        // 进入fastRecover前的cwnd,待ack的seq
        this.m_frcwnd = 0;
        this.m_frSeq = 0;
        this.m_state = Reno.STATE.slowStart;
    }

    get mms() {
        return this.m_mms;
    }

    get cwnd() {
        return this.m_cwnd;
    }

    get rto() {
        return this.m_rto;
    }

    get srtt() {
        return this.m_srtt;
    }

    onAck(ackedSize, pkgStub, sendQueue, ackPkg) {
        if (ackedSize > 0) {
            let now = TimeHelper.uptimeMS();
            if (!pkgStub.package.header.isResend || now - pkgStub.lastSentTime > this.m_rto) {
                let rtt = now - pkgStub.sentTime;
                const alpha = 1/8;
                const beta = 1/4;
                const gama = 4;
                
                if (rtt <= 0) {
                    rtt = this.m_srtt || this.m_rtoMin;
                } else if (rtt > this.m_rtoMax) {
                    rtt = this.m_rtoMax;
                }

                if (this.m_srtt === 0) {
                    this.m_srtt = rtt;
                    this.m_rttvar = rtt / 2;
                } else {
                    let srtt = this.m_srtt;
                    let rttvar = this.m_rttvar;
                    this.m_rttvar = beta*(Math.abs(rtt - srtt)) + (1-beta)*rttvar;
                    this.m_srtt = Math.floor(alpha*rtt + (1-alpha)*srtt);
                }
        
                let rto = Math.ceil(this.m_srtt + gama*this.m_rttvar);
        
                if (rto < this.m_rtoMin) {
                    rto = this.m_rtoMin;
                } else if (rto > this.m_rtoMax) {
                    rto = this.m_rtoMax;
                }
                this.m_rto = rto;
                blog.debug(`[BDT]: bdt transfer(id=${this.m_connection.id}) set rto to ${rto}`);
            }

            if (this.m_state === Reno.STATE.slowStart) {
                const inc = Math.max(ackedSize, this.m_mms);
                blog.debug(`[BDT]: bdt transfer(id=${this.m_connection.id}) increase cwnd with ${inc}`);
                this.m_cwnd += inc;
                if (this.m_cwnd >= this.m_ssthresh) {
                    blog.debug(`[BDT]: bdt transfer(id=${this.m_connection.id}) enter congestion avoid state`);
                    this.m_state = Reno.STATE.congestionAvoid;
                }
            } else if (this.m_state === Reno.STATE.congestionAvoid) {
                let inc = Math.ceil(this.m_mms*this.m_mms/this.m_cwnd);
                this.m_cwnd += inc;
                blog.debug(`[BDT]: bdt transfer(id=${this.m_connection.id}) increase cwnd with ${inc}`);
            } else if (this.m_state === Reno.STATE.fastRecover) {
                // 发生fastRecover时窗口中的所有包都被ack才恢复到拥塞避免
                if (SequenceU32.compare(sendQueue.ackSeq, this.m_frSeq) < 0) {
                    if (sendQueue.resendNext() === 0) {
                        this.m_cwnd = sendQueue.flightSize + this.m_mms;
                    }
                } else {
                    this.m_ssthresh = Math.max(Math.ceil(this.m_frcwnd/2), 2*this.m_mms);
                    this.m_cwnd = this.m_ssthresh;
                    this.m_state = Reno.STATE.congestionAvoid;
                }
                blog.debug(`[BDT]: bdt transfer(id=${this.m_connection.id}) set cwnd with ${this.m_cwnd} from fast recover`);
            }
        } else if (ackedSize === 0) {
            if (sendQueue.dumpCount >= 3) {
                if (sendQueue.dumpCount === 3 && this.m_state !== Reno.STATE.fastRecover) {
                    this.m_frcwnd = this.m_cwnd;
                    this.m_frSeq = sendQueue.maxWaitAckSeq;
                    this.m_state = Reno.STATE.fastRecover;
                    blog.debug(`[BDT]: bdt transfer(id=${this.m_connection.id}) enter fast recover state`);
                }

                blog.debug(`[BDT]: bdt transfer(id=${this.m_connection.id}) set cwnd with ${this.m_cwnd} from fast recover`);
            }
            if (this.m_state === Reno.STATE.fastRecover || (ackPkg.header.finalAck && sendQueue.flightSize > 0)) {
                // 没有resend就发一个新包，保证一个ack后能有一个包进入网络
                if (sendQueue.resendNext(ackPkg.header.finalAck) === 0) {
                    this.m_cwnd = sendQueue.flightSize + this.m_mms;
                }
            }
        }
    }

    onOvertime() {
        this.m_ssthresh = Math.max(Math.ceil(this.m_cwnd/2), 2*this.m_mms);
        let rto = this.m_rto * 2;
        if (rto < this.m_rtoMin) {
            rto = this.m_rtoMin;
        } else if (rto > this.m_rtoMax) {
            rto = this.m_rtoMax;
        }
        this.m_rto = rto;
        blog.debug(`[BDT]: bdt transfer(id=${this.m_connection.id}) set rto to ${rto}`);
        this.m_cwnd = this.m_mms;
        this.m_state = Reno.STATE.slowStart;
        blog.debug(`[BDT]: bdt transfer(id=${this.m_connection.id}) set cwnd with ${this.m_cwnd} from slow start`);
    }
}

Reno.STATE = {
    quickStart: 0,
    slowStart: 1,
    congestionAvoid: 2,
    fastRecover: 3,
};

class BDTTransfer {
    constructor(connection, lastAckCallback) {
        this.m_connection = connection;
        const opt = connection.stack._getOptions();
        // 发送缓存
        this.m_sendBuffer = new BDTSendBuffer(opt.defaultSendBufferSize, opt.drainFreeBufferSize);
        this.m_sendBuffer.on('drain', ()=>{
            setImmediate(() => this.m_connection.emit('drain'));
        });
        // 发送队列
        this.m_sendQueue = new BDTSendQueue(this);
        // nagle 延时
        this.m_nagling = {
            pending: null,
            timeout: opt.nagleTimeout
        };
        // 雍塞窗口大小
        this.m_cwnd = opt.initRecvWindowSize;
        // 接收窗口大小
        this.m_rwnd = opt.initRecvWindowSize;
        // 对端通报的接收窗口大小
        this.m_nrwnd = this.m_rwnd;
        // 接收窗口
        this.m_recvQueue = new BDTRecvQueue(connection);
        this.m_ackTimeout = null;
        
        this.m_cc = new Reno(this.m_connection);
        
        this.m_resendTimer = setInterval(()=>{
            let resendSize = 0;
            const count = this.m_sendQueue.resendWaitAck(this.m_cc.rto, stub => {
                if (resendSize === 0) {
                    this._hasAck();
                    this.m_cc.onOvertime();
                }
    
                resendSize += SequenceU32.delta(stub.package.nextSeq, stub.package.header.seq);
                if (resendSize > this.m_cc.cwnd) {
                    return true; // 一次最多重发窗口大小
                }
            });
        }, opt.resendInterval);

        // send fin的callback，在收到fin ack 时触发
        this.m_finAckCallback = null;
        this.m_finSent = false;
        // 第一次回复了fin的ack时触发
        this.m_lastAckCallback = lastAckCallback;

        this.m_finalAck = this._finalAck();
    }
    
    get connection() {
        return this.m_connection;
    }

    send(buffer) {
        let [err, sentBytes] = this.m_sendBuffer.push(buffer);
        if (sentBytes) {
            this._onWndGrow();
        }
        return sentBytes;
    }

    sendFin(callback) {
        if (this.m_finAckCallback) {
            return ;
        }
        let allocFin = ()=>{
            let encoder = this.m_sendQueue.allocDataPackage(null, true);
            this.m_finSent = true;
            if (encoder) {
                this._postPackage(encoder);
            }
        };
        this.m_finAckCallback = callback;
        if (this.m_sendBuffer.curSize) {
            this.m_sendBuffer.once('empty', ()=>{
                allocFin();
            });
        } else {
            allocFin();
        }

    }

    close() {
        if (this.m_ackTimeout) {
            clearTimeout(this.m_ackTimeout);
            this.m_ackTimeout = null;
        }
        if (this.m_resendTimer) {
            clearInterval(this.m_resendTimer);
            this.m_resendTimer = null;
        }
        this.m_lastAckCallback = null;
        this.m_finalAck.close();
    }

    _onPackage(decoder) {
        if (decoder.header.cmdType === BDTPackage.CMD_TYPE.data
        || decoder.header.cmdType === BDTPackage.CMD_TYPE.fin) {
            let _doAck = immediate => {
                if (immediate || decoder.header.isResend || decoder.header.cmdType === BDTPackage.CMD_TYPE.fin) {
                    this._ackImmediately();
                } else {
                    this._willAck();
                }
            }

            const header = decoder.header;
            let ackSeq = header.ackSeq;
            if (SequenceU32.compare(ackSeq, this.m_sendQueue.sentSeq) > 0) {
                // 比已经发出去的大
                return ;
            }

            let ackType = AckType.data;
            let sack = null;
            if (header.cmdType === BDTPackage.CMD_TYPE.data) {
                if (decoder.header.finalAck) {
                    ackType = AckType.finalAck;
                } else {
                    if (!decoder.data || decoder.header.sack) {
                        ackType = AckType.ack;
                    }
                }
                
                if (decoder.header.sack) {
                    sack = decoder.data;
                }
            }

            if (_DEBUG) {
                if (decoder.body && decoder.body.debug) {
                    let now = Date.now();
                    blog.debug(`bdt transfer(id=${this.m_connection.id}) recv data: senttime:${decoder.body.debug.time},now:${now},consum:${now-decoder.body.debug.time},seq:${decoder.header.seq},ackType:${ackType}`);
                    if (ackType !== AckType.data) {
                        blog.debug(`bdt transfer(id=${this.m_connection.id}) recv data-ack: remote.cc:${JSON.stringify(decoder.body.debug)}`);
                    }
                }
            }

            if (sack) {
                this.m_sendQueue.onSACK(sack);
            }
            let [acked, pkgStub] = this.m_sendQueue.onAckPackage(ackSeq, ackType);
            this.m_cc.onAck(acked, pkgStub, this.m_sendQueue, decoder);
            blog.debug(`bdt transfer(id=${this.m_connection.id}) acked:${acked},cc.state:${this.m_cc.m_state},cc.rto:${this.m_cc.rto},cc.cwnd:${this.m_cc.cwnd},sendQueue.flightSize:${this.m_sendQueue.flightSize}`);
            if (acked >= 0) {
                this.m_nrwnd = decoder.header.windowSize;
                this._onWndGrow();
                if (!this.m_sendQueue.flightSize && this.m_finSent) {
                    if (this.m_finAckCallback) {
                        let finAckCallback = this.m_finAckCallback;
                        finAckCallback();
                    }
                }
            } 
            
            if (ackType !== AckType.data) {
                return ;
            }

            this.m_finalAck.onData();

            let recvQueue = this.m_recvQueue;
            if (SequenceU32.compare(header.seq, recvQueue.nextSeq) < 0) {
                // 收到已经ack的重发包
                _doAck(false);
                return ;
            }
            if (SequenceU32.delta(header.seq, recvQueue.nextSeq) > this.m_rwnd) {
                // 收到接收窗口之外的包
                _doAck(true);
                return ;
            }

            if (_DEBUG) {
                if (decoder.body.debug && decoder.body.debug.length) {
                    assert(decoder.data.length === decoder.body.debug.length, `${decoder.body.debug.length}|${decoder.data.length}`);
                }
                if (decoder.body.debug && decoder.body.debug.md5) {
                    let data = decoder.data || Buffer.concat([]);
                    let decoderMD5 = md5(data);
                    assert(decoderMD5 === decoder.body.debug.md5, `${decoder.body.debug.md5}|${decoderMD5}|${data.toString('hex')}`);
                }
            }

            let unpend = recvQueue.addPackage(decoder);
            if (unpend) {
                _doAck(false);
                let recv = [];
                for (let pkg of unpend) {
                    if (pkg.data && pkg.data.length > 0) {
                        recv.push(pkg.data);
                    }
                }
                if (recv.length) {
                    setImmediate(() => this.m_connection.emit('data', recv));
                }
                if (unpend[unpend.length - 1].header.cmdType === BDTPackage.CMD_TYPE.fin) {
                    if (this.m_lastAckCallback) {
                        setImmediate(() => {
                            // C++的异步tcp模式，通知一个空包
                            this.m_connection.emit('data', [Buffer.allocUnsafe(0)]);
                            // node.js模式，通知'end'
                            this.m_connection.emit('end');
                        });
                        this.m_lastAckCallback();
                        this.m_lastAckCallback = null;
                    }
                }
            } else {
                _doAck(true);
            }
        }
    } 

    _willAck() {
        let opt = this.m_connection.stack._getOptions();
        if (this.m_recvQueue.isQuickAck) {
            return this._ackImmediately();
        }

        let delay = Math.min(this.m_recvQueue.ato, this.m_cc.rto);
        if (!this.m_ackTimeout) {
            this.m_ackTimeout = setTimeout(()=>{
                this.m_ackTimeout = null;
                this._ackImmediately();
            }, delay);
        }
    }

    _ackImmediately(isFinalAck) {
        let encoder = this.m_recvQueue.allocAckPackage(this.m_rwnd);
        if (isFinalAck) {
            encoder.header.finalAck = true;
        }

        if (_DEBUG) {
            encoder.body.debug = {
                cwnd: this.m_cc.cwnd,
                rto: this.m_cc.rto,
                srtt: this.m_cc.srtt,
                state: this.m_cc.m_state,
                ssthresh: this.m_cc.m_ssthresh,
                flightSize: this.m_sendQueue.flightSize,
                sentSeq: this.m_sendQueue.sentSeq,
                ackSeq: this.m_sendQueue.ackSeq,
                sendBuffer: this.m_sendBuffer.curSize,
                ato: this.m_recvQueue.ato,
            }
        }

        this._postPackage(encoder);
    }

    _hasAck() {
        this.m_finalAck.onAcked();
        if (this.m_ackTimeout) {
            clearTimeout(this.m_ackTimeout);
            this.m_ackTimeout = null;
        }
    }

    _onWndGrow(isTimeout) {
        let sendBuffer = this.m_sendBuffer;
        const windowSize = Math.min(this.m_cc.cwnd, this.m_nrwnd);
        const expectPackageSize = Math.min(windowSize >> 2, this.m_cc.mms);
        let maySendBytes = windowSize - this.m_sendQueue.flightSize;
        let sentBytes = 0;

        this.m_sendQueue.resendWaitResend(stub => {
            sentBytes += stub.package.dataLength;
            return sentBytes >= maySendBytes;
        });
        maySendBytes -= sentBytes;

        if (sendBuffer.curSize >= expectPackageSize) {
            isTimeout = false;
            if (this.m_nagling.pending) {
                clearTimeout(this.m_nagling.pending);
                this.m_nagling.pending = null;
            }
        }

        while (maySendBytes > 0) {
            let packageBytes = 0;
            if (maySendBytes >= expectPackageSize) {
                packageBytes = expectPackageSize;
            } else {
                // 剩余空间太小，等空间足够再发送，减少碎片数据块
                break;
            }

            // 剩余数据量太小，等等看是否有新的追加数据
            if (sendBuffer.curSize < expectPackageSize && !isTimeout && !this.m_finAckCallback) {
                if (!this.m_nagling.pending) {
                    this.m_nagling.pending = setTimeout(() => {
                            this.m_nagling.pending=null;
                            this._onWndGrow(true);
                        }, 
                        this.m_nagling.timeout);
                }
                break;
            }

            let buffers = sendBuffer.head(packageBytes);
            if (!buffers) {
                break;
            }
            let encoder = this.m_sendQueue.allocDataPackage(buffers);
            if (encoder) {
                this._postPackage(encoder);
            }
            maySendBytes -= encoder.dataLength;
            sentBytes += encoder.dataLength;
            if (!sendBuffer.curSize) {
                break;
            }
        }  
    }

    // 保底ack，发送一个带reAck标记的ack包
    // 避免对端一个发送窗口全部包丢失后只能等待定时重传,或者本端回复的最后一个ack丢失，
    // 不能直接发纯粹的ack,否则会触发对方进入fastRecover状态，
    // 优化逻辑，待查实标准TCP实现策略后再行改进
    _finalAck() {
        const opt = this.m_connection.stack._getOptions();
        let ackTimeout = Math.max(this.m_cc.srtt, opt.ackTimeoutMax);
        let lastAckTime = TimeHelper.uptimeMS();

        let timer = setInterval(() => {
            let now = TimeHelper.uptimeMS();
            if (now - lastAckTime >= ackTimeout) {
                ackTimeout <<= 1;
                lastAckTime = now;
                this._ackImmediately(true);
            }
        }, opt.ackTimeoutMax);

        const finalAck = {
            onData: () => {
                ackTimeout = Math.max(this.m_cc.srtt, opt.ackTimeoutMax);
            },

            onAcked: () => {
                lastAckTime = TimeHelper.uptimeMS();
            },

            close: () => {
                if (timer) {
                    clearInterval(timer);
                    timer = null;
                }
            },
        };
        return finalAck;
    }

    _postPackage(pkg) {
        if (SequenceU32.compare(pkg.header.ackSeq, this.m_recvQueue.ackSeq) !== 0) {
            this.m_recvQueue.fillAckPackage(pkg, this.m_rwnd);
            pkg.change();
        }

        // <TODO> DEBUG
        if (_DEBUG) {
            pkg.body.debug = pkg.body.debug || {};
            pkg.body.debug.time = Date.now();
            let data = pkg.data;
            pkg.body.debug.length = data.length;
            pkg.body.debug.md5 = md5(data);
            pkg.change();
        }
        
        this.m_connection._postPackage(pkg);
        this._hasAck();
    }
}

BDTTransfer.version = 'v2';
module.exports = BDTTransfer;