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
const {Result: DHTResult, Config, SequenceIncreaseGenerator} = require('./util.js');
const Task = require('./tasks/task.js');
const FindPeerTask = require('./tasks/task_find_peer.js');
const SaveValueTask = require('./tasks/task_save_value.js');
const GetValueTask = require('./tasks/task_get_value.js');
const BroadcastEventTask = require('./tasks/task_broadcast_event.js');
const SplitePackageTask = require('./tasks/task_split_package.js');
const HandshakeTask = require('./tasks/task_handshake.js');

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

const TaskConfig = Config.Task;
const BroadcastConfig = Config.Broadcast;

class TaskExecutor {
    constructor({taskMgr, bucket, packageSender, packageFactory, distributedValueTable}) {
        this.m_bucket = bucket;
        this.m_packageSender = packageSender;
        this.m_packageFactory = packageFactory;
        this.m_distributedValueTable = distributedValueTable;
        this.m_taskMgr = taskMgr;
        this.m_servicePath = '';

        this.m_handshakeTaskMap = new Map(); // <remotePeerid, tasklist>
    }

    findPeer(peerid, isImmediately, callback = null, onStep = null) {
        return this._findPeer(peerid, 0, isImmediately, callback, onStep);
    }

    findRandomPeer(findCount, isImmediately, callback = null, onStep = null, filter = null) {
        return this._findPeer(null, findCount, isImmediately, callback, onStep, filter);
    }

    _findPeer(peerid, findCount, isImmediately, callback = null, onStep = null, filter = null) {
        for (let [taskid, task] of this.m_taskMgr.m_taskMap) {
            if (task.type === 'FindPeerTask'
                && ((task.peerid && task.peerid === peerid) || (!task.peerid && task.findCount === findCount))
                && task.servicePath == this.m_servicePath
                && !task.isComplete
                && filter === task.filter) {
                    // onStep不可忽略空
                    task.addStepListener(onStep);
                    if (callback) {
                        task.addCallback(callback);
                    }
                    return;
            }
        }

        let newTask = new FindPeerTask(this, peerid, isImmediately, findCount, filter);
        newTask.addStepListener(onStep);
        if (callback) {
            newTask.addCallback(callback);
        }
        this.m_taskMgr._run(newTask);
    }

    updateValue(tableName, keyValues, {ttl = 0, isForward = false, timeout = TaskConfig.TimeoutMS, excludePeerids = null} = {}, callback = null) {
        for (let [taskid, task] of this.m_taskMgr.m_taskMap) {
            if (task.type === 'SaveValueTask'
                && task.tableName === tableName
                && task.servicePath == this.m_servicePath
                && !task.isComplete) {
                    keyValues.forEach((v, k) => task.addKeyValue(k, v));
                    task.rebuildPackage();
                    if (callback) {
                        task.addCallback(callback);
                    }
                    return;
            }
        }

        let newTask = new SaveValueTask(this, tableName, keyValues, {ttl, isForward, timeout, excludePeerids});
        if (callback) {
            newTask.addCallback(callback);
        }
        this.m_taskMgr._run(newTask);
    }

    getValue(tableName, keyName, flags, {ttl = 0, isForward = false, timeout = TaskConfig.TimeoutMS, excludePeerids = null} = {}, callback) {
        for (let [taskid, task] of this.m_taskMgr.m_taskMap) {
            if (task.type === 'GetValueTask'
                && task.tableName === tableName
                && task.keyName === keyName
                && task.flags === flags
                && task.servicePath == this.m_servicePath
                && !task.isComplete) {
                    if (callback) {
                        task.addCallback(callback);
                    }
                    return;
            }
        }

        let newTask = new GetValueTask(this, tableName, keyName, flags, {ttl, isForward, timeout, excludePeerids});
        if (callback) {
            newTask.addCallback(callback);
        }
        this.m_taskMgr._run(newTask);
    }

    emitBroadcastEvent(eventName, params, sourcePeer, taskid, {timeout = BroadcastConfig.TimeoutMS} = {}, callback) {
        // LOG_DEBUG(`BROADCAST: servicePath: ${this.servicePath}: eventName: ${eventName}`);
        let task = this.m_taskMgr.m_taskMap.get(taskid);
        if (task) {
            if (callback) {
                task.addCallback(callback);
            }
            return [task, false];
        }

        task = new BroadcastEventTask(this, eventName, params, sourcePeer, taskid, {timeout});
        if (callback) {
            task.addCallback(callback);
        }
        this.m_taskMgr._run(task);
        return [task, true];
    }

    splitPackage(cmdPackage, peer) {
        let newTask = new SplitePackageTask(this, cmdPackage, peer);
        this.m_taskMgr._run(newTask);
    }

    // 主动发起握手
    // 常规流程：先测试直连握手，一个重试周期后，如果还没连上就尝试穿透，下面几个参数可能影响流程
    // agencyPeer: 穿透中介节点，置空则不进行穿透
    // isHoleImmediately: 直连的同时，立即开始穿透；非频繁紧急情况使用，不建议
    // passive：被动连接，先不测试直连，通过agencyPeer通知对方连入；粗测本地网络环境使用
    // handshakeSender: 如果是为了发送特定包而握手，填充该参数，用用户包替换握手包，减少无谓的握手包
    handshakeSource(targetPeer, agencyPeer, isHoleImmediately, passive, handshakeSender, callback) {
        for (let [taskid, task] of this.m_taskMgr.m_taskMap) {
            if (task.type === 'HandshakeSourceTask'
                && task.peerid === targetPeer.peerid) {
                    if (callback) {
                        task.addCallback(callback);
                    }
                    return;
            }
        }

        let bucket = this.bucket;
        let peer = bucket.findPeer(targetPeer.peerid);
        if (peer && !peer.isTimeout(bucket.TIMEOUT_MS)) {
            // 已经connected的,不需要跑握手的任务, 可以直接调用callback
            if ( callback ) {
                callback(DHTResult.SUCCESS, peer)
            }
            return;
        }

        let newTask = new HandshakeTask.Source(this, targetPeer, agencyPeer, isHoleImmediately, passive, handshakeSender);
        if (callback) {
            newTask.addCallback(callback);
        }

        let tasklist = this.m_handshakeTaskMap.get(targetPeer.peerid);
        if (!tasklist) {
            tasklist = [];
            this.m_handshakeTaskMap.set(targetPeer.peerid, tasklist);
        }
        tasklist.push(newTask);

        this.m_taskMgr._run(newTask);
    }

    // 协助握手
    handshakeAgency(srcPeer, targetPeer, taskid) {
        // 按peerid去重，不要按taskid去重，否则，可能被恶意节点请求大批量节点对特定节点发送穿透请求进行DDOS攻击
        for (let [taskid, task] of this.m_taskMgr.m_taskMap) {
            if (task.type === 'HandshakeAgencyTask'
                && task.srcPeerid === srcPeer.peerid
                && task.targetPeerid === targetPeer.peerid) {
                    return;
            }
        }

        let newTask = new HandshakeTask.Agency(this, srcPeer, targetPeer, taskid);
        this.m_taskMgr._run(newTask);
    }

    // 被动握手
    handshakeTarget(srcPeer, taskid) {
        // 按peerid去重，不要按taskid去重，否则，可能被恶意节点请求大批量节点对特定节点发送穿透请求进行DDOS攻击
        for (let [taskid, task] of this.m_taskMgr.m_taskMap) {
            if (task.type === 'HandshakeTargetTask'
                && task.peerid === srcPeer.peerid) {
                    return;
            }
        }

        let newTask = new HandshakeTask.Target(this, srcPeer, taskid);

        let tasklist = this.m_handshakeTaskMap.get(srcPeer.peerid);
        if (!tasklist) {
            tasklist = [];
            this.m_handshakeTaskMap.set(srcPeer.peerid, tasklist);
        }
        tasklist.push(newTask);

        this.m_taskMgr._run(newTask);
    }

    onPackageRecved(cmdPackage, remotePeer, remoteAddr, localAddr) {
        let tasklist = this.m_handshakeTaskMap.get(remotePeer.peerid);
        if (tasklist) {
            tasklist.forEach(task => task.onRemoteResponse(cmdPackage, remotePeer, remoteAddr, localAddr));
        }
    }

    get bucket() {
        return this.m_bucket;
    }

    get packageFactory() {
        return this.m_packageFactory;
    }

    get packageSender() {
        return this.m_packageSender;
    }

    get distributedValueTable() {
        return this.m_distributedValueTable;
    }

    get taskMgr() {
        return this.m_taskMgr;
    }

    get servicePath() {
        return this.m_servicePath;
    }

    set servicePath(path) {
        this.m_servicePath = path;
    }

    genTaskID() {
        return this.m_taskMgr._genTaskID();
    }

    onTaskComplete(task) {
        if (task.type === 'HandshakeSourceTask' || task.type === 'HandshakeTargetTask') {
            this.m_handshakeTaskMap.delete(task.peerid);
        }

        this.m_taskMgr._onTaskComplete(task);
    }
}

class TaskMgr {
    constructor() {
        this.m_idGen = new SequenceIncreaseGenerator();
        this.m_taskMap = new Map();
        this.m_completeTaskIDList = [];
    }

    createTaskExecutor({bucket, packageSender, packageFactory, distributedValueTable}) {
        return new TaskExecutor({taskMgr: this, bucket, packageSender, packageFactory, distributedValueTable});
    }

    getTaskByID(taskID) {
        let task = this.m_taskMap.get(taskID);
        if (task && !task.isComplete) {
            return task;
        }
        return null;
    }

    wakeUpAllTask() {
        for (let taskID of this.m_completeTaskIDList) {
            this.m_taskMap.delete(taskID);
        }
        this.m_completeTaskIDList.length = 0;

        this.m_taskMap.forEach(task => task.wakeUp());
    }

    _genTaskID() {
        let taskID = this.m_idGen.genSeq();

        if (this.m_taskMap.has(taskID)) {
            return this._genTaskID();
        }
        return taskID;
    }

    _run(task) {
        this.m_taskMap.set(task.id, task);
        task.start();
    }

    _onTaskComplete(task) {
        // 可能taskMap正在遍历，尽量不直接修改其中内容
        this.m_completeTaskIDList.push(task.id);
    }
}

module.exports = TaskMgr;