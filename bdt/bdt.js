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
const BDTStack = require('./stack');
const BDTAcceptor = require('./acceptor');
const BDTConnection = require('./connection');
const packageModule = require('./package');

class PeerFinder extends EventEmitter {
    constructor() {
        super();
        this.EVENT = PeerFinder.EVENT;
    }

    findSN(peerid, fromCache, onStep) {
        return Promise.reject('Function PeerFinder.findSN should be overrided.');
    }

    findPeer(peerid) {
        return Promise.reject('Function PeerFinder.findSN should be overrided.');
    }

    getLocalEPList() {
        return [];
    }
}

PeerFinder.EVENT = {
    SNChanged: 'SNChanged'
}

class SimpleSNFinder extends PeerFinder {
    constructor(snPeers) {
        super();
        this.m_snPeers = snPeers;
    }

    findSN(peerid, fromCache, onStep) {
        if (this.m_snPeers && this.m_snPeers.length) {
            return Promise.resolve([packageModule.BDT_ERROR.success, this.m_snPeers]);
        } else {
            return Promise.Promise.resolve([packageModule.BDT_ERROR.invalidState]);
        }
    }
}

function newStack(peerid, eplist, mixSocket, peerFinder, remoteFilter, options) {
    let stack = new BDTStack(peerid, eplist, mixSocket, peerFinder, remoteFilter, options);
    stack.newAcceptor = (acceptorOptions)=>{
        return new BDTAcceptor(stack, acceptorOptions);
    };
    stack.newConnection = (connectionOptions)=>{
        return new BDTConnection(stack, connectionOptions);
    };
    stack.once(BDTStack.EVENT.close, () => {
        stack.newAcceptor = null;
        stack.newConnection = null;
    });
    return stack;
}


module.exports = {
    newStack: newStack,
    Stack: BDTStack,
    Connection: BDTConnection,
    Acceptor: BDTAcceptor,
    ERROR: packageModule.BDT_ERROR,
    Package: packageModule.BDTPackage,
    PeerFinder,
};