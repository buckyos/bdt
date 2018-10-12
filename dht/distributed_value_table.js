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
const {Config, HashDistance, TOTAL_KEY} = require('./util.js');
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

class DistributedValueTableMgr {
    constructor({ TABLE_COUNT = ValueTableConfig.TableCount,
        TABLE_SIZE = ValueTableConfig.TableSize,
        TIMEOUT_MS = ValueTableConfig.ValueTimeoutMS } = {}) {

        this.TABLE_COUNT = TABLE_COUNT;
        this.TABLE_SIZE = TABLE_SIZE;
        this.TIMEOUT_MS = TIMEOUT_MS;

        // <tableName, table>
        this.m_tables = new Map();
        this.m_earlyUpdateTime = 0;
    }

    updateValue(tableName, keyName, value) {
        let table = this.m_tables.get(tableName);
        if (!table) {
            table = new DistributedValueTable();
            this.m_tables.set(tableName, table);
        }

        table.updateValue(keyName, value);
    }

    clearOuttimeValues() {
        let now = TimeHelper.uptimeMS();
        if (now - this.m_earlyUpdateTime <= this.TIMEOUT_MS) {
            return;
        }

        this.m_earlyUpdateTime = now;
        if (this.m_tables.size > this.TABLE_COUNT) {
            let outtimeTableList = [];
            this.m_tables.forEach((table, tableName) => {
                    if (now - table.lastUpdateTime > this.TIMEOUT_MS) {
                        outtimeTableList.push(tableName);
                    }
                });
            outtimeTableList.forEach(tableName => this.m_tables.delete(tableName));
        }

        this.m_tables.forEach((table) => {
                if (now - table.earlyUpdateTime > this.TIMEOUT_MS) {
                    table.knockOut(this.TABLE_SIZE, this.TIMEOUT_MS);
                }

                if (table.earlyUpdateTime < this.m_earlyUpdateTime) {
                    this.m_earlyUpdateTime = table.earlyUpdateTime;
                }
            });
    }

    get tableCount() {
        return this.m_tables.size;
    }

    get valueCount() {
        let count = 0;
        this.m_tables.forEach(table => count += table.valueCount);
        return count;
    }

    findValue(tableName, keyName) {
        let table = this.m_tables.get(tableName);
        if (table) {
            return table.findValue(keyName);
        }
        return null;
    }
    
    findClosestValues(tableName, keyName, {count = ValueTableConfig.FindCloseKeyCount, maxDistance = HashDistance.MAX_HASH} = {}) {
        let table = this.m_tables.get(tableName);
        if (table) {
            return table.findClosestValues(keyName, {count, maxDistance});
        }
        return null;
    }
    
    findLatestValues(tableName, keyName, {count = ValueTableConfig.FindCloseKeyCount, maxDistance = HashDistance.MAX_HASH} = {}) {
        let table = this.m_tables.get(tableName);
        if (table) {
            return table.findLatestValues(keyName, {count, maxDistance});
        }
        return null;
    }

    forEachValue(valueProcess) {
        for (let [tableName, table] of this.m_tables) {
            for (let [keyName, valueObj] of table.values) {
                valueProcess(tableName, keyName, valueObj);
            }
        }
    }

    log() {
        for (let [tableName, table] of this.m_tables) {
            LOG_DEBUG(`Table(${tableName}) count(${table.values.size}):`);
            for (let [keyName, valueObj] of table.values) {
                LOG_DEBUG(`\t${keyName}\t${valueObj.value}`);
            }
        }
    }

}

class DistributedValueTable {
    constructor() {
        this.m_values = new Map();
        this.m_earlyUpdateTime = 0;
        this.m_lastUpdateTime = 0;
    }

    get values() {
        return this.m_values;
    }

    get valueCount() {
        return this.m_values.size;
    }

    get earlyUpdateTime() {
        if (this.m_earlyUpdateTime === 0) {
            let now = TimeHelper.uptimeMS();
            this.m_earlyUpdateTime = now;
            this.m_values.forEach((valueObj, keyName) => {
                if (valueObj.updateTime < this.m_earlyUpdateTime) {
                    this.m_earlyUpdateTime = valueObj.updateTime;
                }
            });
        }

        return this.m_earlyUpdateTime;
    }

    get lastUpdateTime() {
        return this.m_lastUpdateTime;
    }

    updateValue(keyName, value) {
        let now = TimeHelper.uptimeMS();
        let valueObj = this.m_values.get(keyName);
        if (!valueObj) {
            valueObj = {
                value: value,
                keyHash: HashDistance.checkHash(keyName),
                updateTime: now,
            };
            this.m_values.set(keyName, valueObj);
        } else {
            if (this.m_earlyUpdateTime === valueObj.updateTime) {
                this.m_earlyUpdateTime = this.earlyUpdateTime;
            }
            valueObj.value = value;
            valueObj.updateTime = now;
        }

        this.m_lastUpdateTime = now;
    }

    knockOut(timeoutMS) {
        let now = TimeHelper.uptimeMS();
        this.m_earlyUpdateTime = now;
        // timeout
        let outtimeKeyList = [];
        this.m_values.forEach((valueObj, keyName) => {
                if (now - valueObj.updateTime > timeoutMS) {
                    outtimeKeyList.push(keyName);
                } else if (valueObj.updateTime < this.m_earlyUpdateTime) {
                    this.m_earlyUpdateTime = valueObj.updateTime;
                }
            });

        outtimeKeyList.forEach(keyName => this.m_values.delete(keyName));
    }

    findValue(keyName) {
        if (keyName === TOTAL_KEY) {
            let keyValues = new Map();
            this.m_values.forEach((valueObj, key) => keyValues.set(key, valueObj.value));
            return keyValues;
        }

        let valueObj = this.m_values.get(keyName);
        if (valueObj) {
            return new Map([[keyName, valueObj.value]]);
        }
        return null;
    }

    findClosestValues(keyName, {count = ValueTableConfig.FindCloseKeyCount, maxDistance = HashDistance.MAX_HASH} = {}) {
        LOG_ASSERT(count >= 0, `Try find negative(${count}) values.`);
        if (count < 0) {
            return new Map();
        }

        let hash = HashDistance.checkHash(keyName);
        let foundValueList = [];
        for (let [key, valueObj] of this.m_values) {
            let curValueDistance = HashDistance.calcDistanceByHash(valueObj.keyHash, hash);
            if (HashDistance.compareHash(curValueDistance, maxDistance) > 0) {
                continue;
            }

            let farthestValue = foundValueList.length > 0? foundValueList[foundValueList.length - 1] : null;
            if (foundValueList.length < count
                || HashDistance.compareHash(curValueDistance, HashDistance.calcDistanceByHash(farthestValue.valueObj.keyHash, hash)) < 0) {
                let done = false;
                for (let j = 0; j < foundValueList.length; j++) {
                    if (HashDistance.compareHash(curValueDistance, HashDistance.calcDistanceByHash(foundValueList[j].valueObj.keyHash, hash)) < 0) {
                        foundValueList.splice(j, 0, {valueObj, key});
                        done = true;
                        if (foundValueList.length > count) {
                            foundValueList.pop();
                        }
                        break;
                    }
                }
                if (!done) {
                    foundValueList.push({valueObj, key});
                }
            }
        }

        let foundValueTable = new Map();
        foundValueList.forEach(item => foundValueTable.set(item.key, item.valueObj.value));
        return foundValueTable;
    }

    findLatestValues(keyName, {count = ValueTableConfig.FindCloseKeyCount, maxDistance = HashDistance.MAX_HASH} = {}) {
        LOG_ASSERT(count >= 0, `Try find negative(${count}) values.`);
        if (count < 0) {
            return new Map();
        }

        let hash = HashDistance.checkHash(keyName);
        let foundValueList = [];
        for (let [key, valueObj] of this.m_values) {
            let curValueDistance = HashDistance.calcDistanceByHash(valueObj.keyHash, hash);
            if (HashDistance.compareHash(curValueDistance, maxDistance) > 0) {
                continue;
            }

            let latestValue = foundValueList.length > 0? foundValueList[foundValueList.length - 1] : null;
            if (foundValueList.length < count
                || valueObj.updateTime < latestValue.valueObj.updateTime) {
                let done = false;
                for (let j = 0; j < foundValueList.length; j++) {
                    if (valueObj.updateTime < foundValueList[j].valueObj.updateTime) {
                        foundValueList.splice(j, 0, {valueObj, key});
                        done = true;
                        if (foundValueList.length > count) {
                            foundValueList.pop();
                        }
                        break;
                    }
                }
                if (!done) {
                    foundValueList.push({valueObj, key});
                }
            }
        }

        let foundValueTable = new Map();
        foundValueList.forEach(item => foundValueTable.set(item.key, item.valueObj.value));
        return foundValueTable;
    }}

module.exports = DistributedValueTableMgr;