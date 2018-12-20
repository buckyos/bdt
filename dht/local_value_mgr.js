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

const Base = require('../base/base.js');
const {Config, TOTAL_KEY} = require('./util.js');
const BaseUtil = require('../base/util.js');
const TimeHelper = BaseUtil.TimeHelper;

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

const ValueTableConfig = Config.ValueTable;
const HashConfig = Config.Hash;

class LocalValueMgr {
    constructor({taskExecutor}) {
        // <tableName, table>
        this.m_tables = new Map();

        this.m_taskExecutor = taskExecutor;
    }

    saveValue(tableName, keyName, value) {
        let table = this.m_tables.get(tableName);
        if (!table) {
            table = new Map();
            this.m_tables.set(tableName, table);
        } else {
            let valueObj = table.get(keyName);
            if (valueObj) {
                valueObj.value = value;
                valueObj.updateTime = 0;
                return;
            }
        }

        table.set(keyName, {value, updateTime: 0});
    }

    deleteValue(tableName, keyName) {
        if (keyName === TOTAL_KEY) {
            this.m_tables.delete(tableName);
            return;
        }

        let table = this.m_tables.get(tableName);
        if (table) {
            if (table.delete(keyName) && table.size === 0) {
                this.m_tables.delete(tableName);
            }
        }
    }

    getValue(tableName, keyName) {
        if (keyName === TOTAL_KEY) {
            let table = this.m_tables.get(tableName);
            if (table) {
                let keyValues = new Map();
                table.forEach((valueObj, key) => keyValues.set(key, valueObj.value));
                return keyValues;
            }
            return null;
        }

        let table = this.m_tables.get(tableName);
        if (table) {
            let valueObj = table.get(keyName);
            if (valueObj) {
                return new Map([[keyName, valueObj.value]]);
            }
        }
        return null;
    }

    updateToRemote({outtimeMS = ValueTableConfig.ValueUpdateIntervalMS} = {}) {
        let now = TimeHelper.uptimeMS();
        for (let [tableName, table] of this.m_tables) {
            let values = new Map();
            for (let [key, valueObj] of table) {
                if (now - valueObj.updateTime >= outtimeMS) {
                    valueObj.updateTime = now;
                    values.set(key, valueObj.value);
                }
            }

            if (values.size > 0) {
                this.m_taskExecutor.updateValue(tableName, values, {ttl: 1, isForward: false});
            }
        }
    }

    log() {
        for (let [tableName, table] of this.m_tables) {
            LOG_DEBUG(`[DHT${this.m_taskExecutor.packageFactory.appid}] Table(${tableName}) count(${table.size}):`);
            for (let [keyName, valueObj] of table) {
                LOG_DEBUG(`\t${keyName}\t${valueObj.value}`);
            }
        }
    }

}

module.exports = LocalValueMgr;