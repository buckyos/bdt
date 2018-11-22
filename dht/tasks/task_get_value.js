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
const DestributedValueTable = require('../distributed_value_table.js');
const {TouchNodeConvergenceTask} = require('./task_touch_node_recursion.js');
const DHTPackage = require('../packages/package.js');
const DHTCommandType = DHTPackage.CommandType;

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

const SaveValueConfig = Config.SaveValue;
const TaskConfig = Config.Task;

class GetValueTask extends TouchNodeConvergenceTask {
    constructor(owner, tableName, keyName, flags, {ttl = 0, isForward = false, timeout = TaskConfig.TimeoutMS, excludePeerids = null} = {}) {
        super(owner, {ttl, isForward, timeout, excludePeerids});

        this.m_tableName = tableName;
        this.m_keyName = keyName;
        this.m_flags = flags;

        this.m_values = null;
    }

    get tableName() {
        return this.m_tableName;
    }

    get keyName() {
        return this.m_keyName;
    }

    get flags() {
        return this.m_flags;
    }

    _processImpl(response, remotePeer) {
        LOG_INFO(`LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) remotePeer:${response.common.src.peerid} responsed GetValue(${this.m_tableName}:${this.m_keyName}:${this.m_flags})`);
        if (response.body.values) {
            if (response.body.r_nodes && Array.isArray(response.body.r_nodes)) {
                response.body.r_nodes.forEach(peerid => {
                    if (typeof peerid === 'string' && peerid.length > 0) {
                        this.m_arrivePeeridSet.add(peerid);
                    }
                });
            }
            this.m_arrivePeeridSet.add(response.src.peerid);

            this.m_values = new Map(response.body.values);
            this._onComplete(DHTResult.SUCCESS);
        } else {
            super._processImpl(response, remotePeer);
        }
    }

    _onCompleteImpl(result) {
        LOG_INFO(`LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) complete GetValue(count=${this.m_values? this.m_values.size : 0})`);
        this._callback(result, this.m_values, this.m_arrivePeeridSet);
        super.destroy();
    }

    _createPackage() {
        let cmdPackage = this.packageFactory.createPackage(DHTCommandType.FIND_VALUE_REQ);

        cmdPackage.body = {
            taskid: this.m_id,
            flags: this.m_flags,
            tableName: this.m_tableName,
            key: this.m_keyName,
        };
        return cmdPackage;
    }

    get _targetKey() {
        return this.m_tableName;
    }

    _stopImpl() {
    }
}

module.exports = GetValueTask;