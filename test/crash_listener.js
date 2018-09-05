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

const path = require("path");
const fs = require("fs-extra");
const Base = require('../base/base.js');

const LOG_INFO = Base.BX_INFO;
const LOG_WARN = Base.BX_WARN;
const LOG_DEBUG = Base.BX_DEBUG;
const LOG_CHECK = Base.BX_CHECK;
const LOG_ASSERT = Base.BX_ASSERT;
const LOG_ERROR = Base.BX_ERROR;

class CrashListener {
    constructor() {
        this.m_logFolder = null;
        this.m_crashed = false;
        this.m_onCrashCallback = null;
    }

    listen(onCrash) {
        this.m_onCrashCallback = onCrash;
        process.on('unhandledRejection', err => this._onCrash(err))
        process.on('uncaughtException', err => this._onCrash(err));
    }

    enableFileLog(logFolder) {
        this.m_logFolder = logFolder;
    }

    _onCrash(err) {
       if (this.m_crashed) {
            process.exit(-1);
            return;
        }

        this.m_crashed = true;
        let errFileName = path.basename(require.main.filename, ".js");
        if (!errFileName || errFileName.length <= 0) {
            errFileName = "node";
        }
        errFileName += '_crash_[' + process.pid + '].err';

        let content = "crash time: " + Base.TimeFormater.getFormatTime();
        LOG_ERROR(content);
        let errStack = err.stack;
        // errStack = Base.BaseLib.replaceAll(errStack, '\r', ' ');
        // errStack = Base.BaseLib.replaceAll(errStack, '\n', ' ');
        LOG_ERROR(err.stack);
        content += Base.blog.getOptions().getFormatter().getLineBreak();
        content += err.stack;
        LOG_ERROR(content);

        let onCrashResult = '';
        if (this.m_onCrashCallback) {
            onCrashResult = this.m_onCrashCallback(err);
        }

        Promise.resolve(onCrashResult).then(() => {
            if (this.m_logFolder) {
                Base.BaseLib.mkdirsSync(this.m_logFolder + '/errors');
                fs.writeFileSync(this.m_logFolder + '/errors/' + errFileName, content);
            }
            process.exit(-1);
        });
    }
}

module.exports = CrashListener;