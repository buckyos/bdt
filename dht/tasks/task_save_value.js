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

class SaveValueTask extends TouchNodeConvergenceTask {
    constructor(owner, tableName, keyValueMap, {ttl = 0, isForward = false, timeout = TaskConfig.TimeoutMS, excludePeerids = null} = {}) {
        super(owner, {ttl, isForward, timeout, excludePeerids});
        this.m_tableName = tableName;
        this.m_keyValueMap = keyValueMap;

        this.m_package = null;
    }

    get tableName() {
        return this.m_tableName;
    }

    addKeyValue(key, value) {
        this.m_keyValueMap.set(key, value);
    }

    rebuildPackage() {
        this.m_package = this._createPackage();
        if (this.servicePath && this.servicePath.length > 0) {
            this.m_package.body.servicePath = this.servicePath;
        }
    }

    _processImpl(response, remotePeer) {
        LOG_INFO(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) SaveValue (${this.m_tableName}) to ${remotePeer.peerid}`);
        super._processImpl(response, remotePeer);
    }

    _onCompleteImpl(result) {
        LOG_INFO(`[DHT(${this.packageFactory.appid})] LOCALPEER:(${this.bucket.localPeer.peerid}:${this.servicePath}) SaveValue complete(${this.m_tableName})`);
        this._callback(result, this.m_arrivePeeridSet);
        super.destroy();
    }

    _createPackage() {
        let cmdPackage = this.packageFactory.createPackage(DHTCommandType.UPDATE_VALUE_REQ);

        cmdPackage.body = {
            taskid: this.m_id,
            tableName: this.m_tableName,
            values: [...this.m_keyValueMap],
        };
        return cmdPackage;
    }

    get _targetKey() {
        return this.m_tableName;
    }

    _stopImpl() {
    }

    get _isExcludeLocalPeer() {
        return false;
    }
}

module.exports = SaveValueTask;