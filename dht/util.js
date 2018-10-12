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

const BaseUtil = require('../base/util.js');

const Result = {
    SUCCESS: 0,
    PENDING: 1,
    STOPPED: 2,
    FAILED: 3,
    TIMEOUT: 4,
    INVALID_ARGS: 5,
    INVALID_PACKAGE: 6,
    ABORT: 7,
};

const MAX_SAFE_INTEGER = 0xFFFFFFFFFFFFF;
const MAX_UINT32 = 0xFFFFFFFF;

const Config = {
    Hash: {
        BitCount: BaseUtil.HashDistance.HASH_BIT_COUNT,
    },

    Peer: {
        epTimeout: 60809,
        symEPCount: 5,
        NATTypeTime: 600809,
        recommandNeighborTime: 10000, // 上线10秒钟内可能推荐一次邻居
    },

    Bucket: {
        BucketCount: 16,
        BucketSize: 8,
        PeerTimeoutMS: 608090,
        FindPeerCount: 8,
        ResponcePeerCount: 3, // 响应搜索返回peer数
    },
    
    ValueTable: {
        TableCount: 16,
        TableSize: 8,
        TotalValueCount: 128,
        ValueTimeoutMS: 1500809,
        ValueUpdateIntervalMS: 600809,
        FindCloseKeyCount: 5,
    },

    FindPeer: {
        StepTimeout: 500, // 在一段时间内没有节点响应FindPeer，就通知一次Step
    },

    SaveValue: {
        DupCount: 5,
    },

    GetValue: {
    },

    Broadcast: {
        TimeoutMS: 600809,
        LimitPeerCountOnce: 8, // 一次同时通知peer数
    },

    Package: {
        MagicNum: 0x8084,
        MaxTTL: 1,
        MinSeq: 1,
        MaxSeq: MAX_UINT32,
        Timeout: 10000,
        RetryInterval: 1000,
        RetryTimes: 3,
        InitRRT: 1000,
    },

    Task: {
        TimeoutMS: 600809,
        MaxIdleTimeMS: 500,
        MinTaskID: 1,
        MaxTaskID: MAX_UINT32,
    },

    Handshake: {
        TimeoutMS: 5000,
    },

    RouteTable: {
        ExpandIntervalMS: {
            Min: 2000,
            Max: 600809,
            dynamic(peerCount, peerDelta) { // 根据当前peer数和上次扩充peer增量动态调整扩充频率
                let interval = 600809;
                if (peerCount <= 16) {
                    interval = 2000;
                } else if (peerCount <= 32) {
                    interval = 300809;
                }

                if (peerDelta <= 4) {
                    interval = Math.max(interval, 300809);
                } else if (peerDelta <= 8) {
                    interval *= 2;
                }
                return Math.min(interval, Config.RouteTable.ExpandIntervalMS.Max);
            },
        },
        PingIntervalMS: {
            Min: 40809,
            Max: 540809,
            Retry: 10000,
            dynamic(distRank) { // 根据与目标peer的距离动态调整ping间隔
                let ms = Config.RouteTable.PingIntervalMS.Min + 20000 * distRank;
                return Math.min(ms, Config.RouteTable.PingIntervalMS.Max);
            }
        }
    }
};

const RandomGenerator = {
    // 默认去掉了容易混淆的字符oOLl,9gq,Vv,Uu,I1
    CHAR_SET: 'ABCDEFGHJKMNPQRSTWXYZabcdefhijkmnprstwxyz2345678',

    string(length = 32) {
        let maxPos = RandomGenerator.CHAR_SET.length;
        let result = '';
        for (let i = 0; i < length; i++) {
            result += RandomGenerator.CHAR_SET.charAt(RandomGenerator.integer(maxPos));
        }
        return result;
    },

    integer(max, min = 0) {
        let result = Math.round(Math.random() * (max - min)) + min;
        if (result > max) {
            result = max;
        }
        return result;
    }
};

class SequenceIncreaseGenerator {
    constructor(lowBound, upBound) {
        this.m_lowBound = lowBound;
        this.m_upBound = upBound;
        this.m_nextSeq = RandomGenerator.integer(upBound, lowBound);
    }

    genSeq() {
        let seq = this.m_nextSeq++;
        if (this.m_nextSeq > this.m_upBound) {
            this.m_nextSeq = this.m_lowBound;
        }
        return seq;
    }
}

// SaveValue(tableName, USER_DEFINED_KEY, value) : table[USER_DEFINED_KEY] = value
// SaveValue(tableName, TOTAL_KEY, value): not support 不支持更新整个表，防止有人恶意刷新整个表导致整个表内容丢失
// DeleteValue(tableName, USER_DEFINED_KEY) : table.delete(USER_DEFINED_KEY) 删除本地发起SaveValue的USER_DEFINED_KEY
// DeleteValue(tableName, TOTAL_KEY): 删除所有本地发起SaveValue的表名为tableName的数据
// GetValue(tableName, USER_DEFINED_KEY): return table[USER_DEFINED_KEY]
// GetValue(tableName, TOTAL_KEY): return table
const USER_DEFINED_KEY = 'DHTValueTable.UserDefined';
const TOTAL_KEY = 'DHTValueTable.TotalTable';

const GetValueFlag = {
    Precise: 0x0,
    KeyHashClose: 0x1,
    UpdateLatest: 0x2,
};

module.exports.Result = Result;
module.exports.MAX_SAFE_INTEGER = MAX_SAFE_INTEGER; // 2^53-1 最大安全整数
module.exports.MAX_UINT32 = MAX_UINT32;
module.exports.GetValueFlag = GetValueFlag;
module.exports.TOTAL_KEY = TOTAL_KEY;
module.exports.Config = Config;
module.exports.SequenceIncreaseGenerator = SequenceIncreaseGenerator;
module.exports.EndPoint = BaseUtil.EndPoint;
module.exports.RandomGenerator = RandomGenerator;
module.exports.HashDistance = BaseUtil.HashDistance;
