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
const DHTPackageFactory = require('./package_factory.js');
const {Config} = require('./util.js');
const BaseUtil = require('../base/util.js');
const TimeHelper = BaseUtil.TimeHelper;

const PackageConfig = Config.Package;

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

class PiecePackageRebuilder {
    constructor() {
        this.m_packageRebuildingPeerTaskMgrs = new Map();
    }

    onGotNewPiece(splitPackageReq) {
        let peerid = splitPackageReq.body.peerid;
        if (typeof peerid !== 'string' || peerid.length === 0) {
            return;
        }
        let rebuildingTaskMgr = this.m_packageRebuildingPeerTaskMgrs.get(peerid);
        if (!rebuildingTaskMgr) {
            rebuildingTaskMgr = new PackageRebuildingPeerTaskMgr(peerid);
            this.m_packageRebuildingPeerTaskMgrs.set(peerid, rebuildingTaskMgr);
        }

        let originalPackageBuffer = rebuildingTaskMgr.onGotNewPiece(splitPackageReq);
        if (originalPackageBuffer) {
            if (rebuildingTaskMgr.taskCount === 0) {
                this.m_packageRebuildingPeerTaskMgrs.delete(peerid);
            }
            return originalPackageBuffer;
        }
        return null;
    }

    clearTimeoutTasks() {
        let emptyPeerids = [];

        this.m_packageRebuildingPeerTaskMgrs.forEach((rebuildingTaskMgr, peerid) => {
            rebuildingTaskMgr.clearTimeoutTasks();
            if (rebuildingTaskMgr.taskCount === 0) {
                emptyPeerids.push(peerid);
            }
        });

        emptyPeerids.forEach(peerid => this.m_packageRebuildingPeerTaskMgrs.delete(peerid));
    }
}

class PackageRebuildingPeerTaskMgr {
    constructor(peerid) {
        this.m_tasks = new Map();
    }

    onGotNewPiece(piecePkg) {
        let taskid = piecePkg.body.taskid;
        if (!taskid) {
            return;
        }
        let task = this.m_tasks.get(taskid);
        if (!task) {
            task = new PackageRebuildingTask(taskid, piecePkg.body.max + 1);
            this.m_tasks.set(taskid, task);
        }

        let originalPackageBuffer = task.onGotNewPiece(piecePkg);
        if (originalPackageBuffer) {
            this.m_tasks.delete(taskid);
            return originalPackageBuffer;
        }
        return null;
    }

    get taskCount() {
        return this.m_tasks.size;
    }

    clearTimeoutTasks() {
        const now = TimeHelper.uptimeMS();
        let timeoutTasks = [];
        this.m_tasks.forEach((task, taskid) => {
            if (now - task.activeTime > PackageConfig.Timeout) {
                timeoutTasks.push(taskid);
            }
        });

        timeoutTasks.forEach(taskid => this.m_tasks.delete(taskid));
    }
}

class PackageRebuildingTask {
    constructor(taskid, pieceCount) {
        this.m_piecePkgs = Array.from({length: pieceCount});
        this.m_gotPieceCount = 0;
        this.m_activeTime = 0;
    }

    onGotNewPiece(piecePkg) {
        this.m_activeTime = TimeHelper.uptimeMS();

        let pieceNo = piecePkg.body.no;
        LOG_ASSERT(piecePkg.body.max + 1 === this.m_piecePkgs.length,
            `[DHT(${piecePkg.appid})] Splite package max-no conflict: (max:${piecePkg.body.max}, pieceCount:(${this.m_piecePkgs.length}))`);
        if (!this.m_piecePkgs[pieceNo]) {
            this.m_piecePkgs[pieceNo] = piecePkg;
            this.m_gotPieceCount++;
        }

        if (this.m_gotPieceCount === this.m_piecePkgs.length) {
            let buffers = Array.from({length: this.m_piecePkgs.length});
            this.m_piecePkgs.forEach(piece => buffers[piece.body.no] = piece.body.buf);
            let orignalPkgBuffer = Buffer.concat(buffers);
            return orignalPkgBuffer;
        }
        return null;
    }

    get activeTime() {
        return this.m_activeTime;
    }

    set activeTime(newValue) {
        this.m_activeTime = newValue;
    }
}

module.exports = PiecePackageRebuilder;