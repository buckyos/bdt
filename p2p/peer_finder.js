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

const EventEmitter = require('events');
const DHT = require('../dht/dht.js');
const SNDHT = require('../sn/sn_dht.js');
const BDT = require('../bdt/bdt.js');

class PeerFinder extends BDT.PeerFinder {
    constructor(snPeer, snDHT, dht) {
        super();

        this.snDHT = snDHT;
        this.snPeer = snPeer;
        this.dht = dht;
    }

    get snPeer() {
        return this.m_snPeer;
    }

    set snPeer(snPeer) {
        this.m_snPeer = snPeer;
        setImmediate(() => this.emit(this.EVENT.SNChanged));
    }

    get dht() {
        return this.m_dht;
    }
        
    set dht(_dht) {
        this._destroyDHT();

        this.m_dht = _dht;

        if (this.m_dht) {
            this.m_dht.once(DHT.EVENT.stop, () => this._destroyDHT());
            setImmediate(() => this.emit(this.EVENT.SNChanged));
        }
    }

    get snDHT() {
        return this.m_snDHT;
    }

    set snDHT(_dht) {
        this._destroySNDHT();

        if (_dht) {
            this.m_snDHT = new SNDHT(_dht);
            this.m_snDHT.signinVistor();
            _dht.once(DHT.EVENT.stop, () => this._destroySNDHT());
            setImmediate(() => this.emit(this.EVENT.SNChanged));
        }
    }

    getLocalEPList() {
        if (this.m_dht) {
            return this.m_dht.localPeer.eplist;
        }
        return [];
    }

    destory() {
        this._destroyDHT();
        this._destroySNDHT();
    }

    findSN(peerid, fromCache, onStep) {
        if (this.m_snPeer) {
            let snPeerArray = this.m_snPeer;
            if (!Array.isArray(this.m_snPeer)) {
                snPeerArray = [this.m_snPeer];
            }
            return Promise.resolve([BDT.ERROR.success, snPeerArray]);
        } else if (this.m_snDHT) {
            return new Promise(resolve => {
                this.m_snDHT.findSN(peerid,
                    fromCache,
                    ({result, snList}) => {
                            if (onStep) {
                                return onStep([result, snList]);
                            }
                        },
                    ({result, snList}) => {
                            if (result) {
                                resolve([result]);
                            } else {
                                resolve([BDT.ERROR.success, snList]);
                            }
                        });
            });
        } else {
            return Promise.resolve([BDT.ERROR.invalidState]);
        }
    }

    findPeer(peerid) {
        if (this.m_dht) {
            return new Promise(resolve => {
                this.m_dht.findPeer(peerid, null, ({result, peerlist}) => {
                    let peer = null;
                    if (peerlist && peerlist.length > 0 && peerlist[0].peerid === peerid) {
                        peer = peerlist[0];
                    }
                    resolve(peer);
                });
            });
        } else {
            // 表示不支持直接搜索peer
            return null;
        }
    }

    _destroySNDHT() {
        if (this.m_snDHT) {
            this.m_snDHT.signoutVistor();
            this.m_snDHT = null;
        }
    }

    _destroyDHT() {
        if (this.m_dht) {
            this.m_dht = null;
        }
    }
}

PeerFinder.EVENT = {
    SNChanged: 'SNChanged'
}

module.exports = PeerFinder;