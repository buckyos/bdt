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

/**
 * 本文件只描述项目中关键接口的功能、参数和用法，只起到示意作用，是一个较好的学习材料；
 * 不提供任何功能实现，开发者也不要引用该文件
 */

class P2P extends EventEmitter {
    /* 
        一步创建一个启动了BDT协议栈的P2P对象，封装了create=>joinDHT|snPeer=>startupBDTStack的过程；
        一般情况使用这个接口就好了

        params:
            peerid:string peer id
            udp: {
                addrList:string[] local address list
                initPort:number initial udp port    默认：0，随机PORT
                maxPortOffset:number max try bind port offset   默认：0；initPort=0时，忽略该参数
            }
            tcp: {
                addrList:string[] local address list
                initPort:number initial udp port    默认：0，随机PORT
                maxPortOffset:number max try bind port offset   默认：0；initPort=0时，忽略该参数
            }
            snPeer: [{
                peerid:
                eplist:
            }]
            dhtEntry: [{
                peerid:
                eplist
            }],
            listenerEPList: [ep1,ep2,...]   用户指定的监听EP；
                                            NAT环境下，无法通过udp.addrList和tcp.addrList获知本地PEER的公网访问地址；
                                            可以通过这个参数指定本地PEER的公网访问地址；
                                            如果不指定，则会通过主动对其他PEER的访问响应包分析其公网地址
            options: {

            }
    */
    static create4BDTStack(params, callback = null);
        
    /*
        创建一个P2P对象
        params:
            peerid:string peer id   必填
            udp: {
                addrList:string[] local address list
                initPort:number initial udp port    默认：0，随机PORT
                maxPortOffset:number max try bind port offset   默认：0；initPort=0时，忽略该参数
            }
            tcp: {
                addrList:string[] local address list
                initPort:number initial udp port    默认：0，随机PORT
                maxPortOffset:number max try bind port offset   默认：0；initPort=0时，忽略该参数
            }
            listenerEPList: [ep1,ep2,...]   用户指定的监听EP；
                                            NAT环境下，无法通过udp.addrList和tcp.addrList获知本地PEER的公网访问地址；
                                            可以通过这个参数指定本地PEER的公网访问地址；
                                            如果不指定，则会通过主动对其他PEER的访问响应包分析其公网地址
    */
    static create(params, callback = null);
    
        // 关闭P2P所有功能
    close();

    // local peerid
    get peerid();

    // 如果要通过dht网络检索peer，要调用joinDHT加入dht网络
    // dhtEntryPeers: [{peerid: xxx, eplist:[4@ip@port]}]
    joinDHT(dhtEntryPeers);

    // 退出DHT网络，如果没有设定固定SN，将无法连接到任何peer
    disjoinDHT();

    // 如果使用固定的SN检索peer，要设置snPeer
    // {peerid: xxx, eplist:[4@ip@port]}
    set snPeer(snPeer);

    // 启动bdt协议栈，在此之前须设置snPeer属性或者调用joinDHT加入DHT网络
    // options指定一些影响bdt运行的参数，建议置null采用默认值
    startupBDTStack(options, callback = null);

    // 启动SN服务
    startupSNService();

    // 如果没有加入DHT网络，将返回null
    // 如果加入了DHT网络，将返回代表该DHT网络的对象，通过它，你能实现以下功能：
    // 1.能找到指定peerid的远程peer；
    // 2.在DHT网络中创建表，并在表中写入数据；或者从指定表中读取数据；
    // 3.向DHT网络中随机peer广播一个事件，直到收到的peer数达到指定值
    // DHT对象的具体接口参考下方DHT类
    get dht();

    // 如果没启动BDT协议栈，将返回null
    // 如果成功启动了BDT协议栈，将返回代表该协议栈的BDTStack对象，通过它，你能实现以下功能：
    // 1.实现一个服务端程序：监听一个vport，等待客户端连接，并通信
    // 2.实现一个客户端程序：对远程服务端发起连接，并通信
    // BDTStack对象的具体接口参考下方BDTStack类
    get bdtStack();

    // 如果没有在本地启动SN服务，将返回null
    // 如果成功在本地启动了SN服务，将返回代表该服务的SN对象，通过它，你能实现一个简单的SN服务，
    // 主要有peer地址检索和内网穿透功能，可以辅助你自己的分布式应用实现peer-peer的连接；
    // SN对象的具体接口参考下方SN类
    get snService();
}

// 事件列表
P2P.EVENT = {
    create: 'create',
    close: 'close',
    BDTStackCreate: 'BDTStackCreate',
    SNStart: 'SNStart',
};

// 错误码
P2P.ERROR = {
    success: 0,
    conflict: 1,
    invalidState: 2,
    timeout: 3,
    outofSize: 4,
    dhtError: 5,
    invalidPackage: 6,
    unmatchPackage: 7, 
    toString(err);
};

/**
 * DHT输入输出PEERINFO结构
 *  PEERINFO: {
 *      peerid: string,
 *      eplist: ARRAY[ep_string],
 *  }
 */
class DHT extends EventEmitter {

    // 退出DHT网络
    stop();

    // 加入DHT网络中活跃的peer信息，用于丰富本地路由表，典型应用场景有两个：
    // 1.刚启动时，本地路由表为空，需要传入默认的初始节点或者上次运行过程中发现的节点列表；
    // 2.本地路由表中所有peer都无法触达的极端情况，用户从互联网或者其他渠道获得当前活跃的peer列表；
    // remotePeerInfo：PEERINFO
    activePeer(remotePeerInfo);
    
    // 本地peer信息
    get localPeer();

    // callback({result, peerlist})
    // 查询跟指定peerid相似的peer信息，可能会返回多个peer，如果查到peerid跟指定peerid完全相同的peer，将会在peerlist[0]返回
    findPeer(peerid, callback);

    // 向指定表(tableName)的指定键值(keyName)写入数据(value);
    // 写入后会定时向peerid和tableName最相似的几个peer推送该value，其他peer不一定能立即访问到；
    // value可以是数值/字符串/对象/数组
    // 注意：keyName = 'DHTValueTable.TotalTable'是保留字，表示该操作是针对整个table的，不能用于saveValue；
    saveValue(tableName, keyName, value);

    // 删除saveValue写入的数据，停止向保存该数据的peer推送，等待数据超时，在超时前，其他peer仍然能访问到该数据；
    // 注意：keyName = 'DHTValueTable.TotalTable'，表示整体删除本地通过saveValue写入的指定表，其他peer向指定表中写入的key-value无法删除
    deleteValue(tableName, keyName);

    // callback({result, values: Map<key, value>})
    // 查询指定表指定键值上的值
    // 注意：keyName = 'DHTValueTable.TotalTable'，表示要查询整张表的内容
    // (flags & 0x1) = 0x1，表示要求精确返回指定table指定key上的value
    // (flags & 0x1) = 0x0，表示要求返回与指定key相似的若干key-value
    getValue(tableName, keyName, flags, callback);

    // callback({result, arrivedCount})
    // 向DHT网络上所有节点发送广播事件，直到有指定数量的节点收到该事件
    emitBroadcastEvent(eventName, params, arriveNodeCount, callback);

    // listener(eventName, params, sourcePeer)
    // 监听DHT广播事件
    attachBroadcastEventListener(eventName, listener);

    // attachBroadcastEventListener相同输入参数
    // 取消监听DHT广播事件
    detachBroadcastEventListener(eventName, listener);

    // 获取本地路由表中所有在线peer列表
    getAllOnlinePeers();

    // @param  <number> count 期望找多少个peer
    // @param  <boolean> fromLocal 是否只从本地路由表中搜索
    // @param  <function> callback({dht, result, peerlist}) 完成回调
    // @param  <object> options: {
    //    <function> onStep({dht, result, peerlist}), 定时返回已经搜索到的全部peer列表，搜索完成时间可能比较长，部分要求及时响应的需求可以处理该事件，返回true停止搜索
    //    <function> filter(peer)，过滤搜索到的节点，返回true表示该peer有效
    // }
    getRandomPeers(count, fromLocal, callback, options);
}

// DHT发生事件列表
DHT.EVENT = {
    start: 'start',
    stop: 'stop',
};

class BDTStack extends EventEmitter {

    // 状态
    get state();

    // 本地peerid
    get peerid();

    // 本地启用的eplist
    get eplist();

    // params = {vport: number}
    // 服务端构造一个acceptor，具体接口参考BDTAcceptor类
    newAcceptor(params);

    // 客户端构造一个connection对象，准备发起连接，具体接口参考BDTConnection类
    newConnection();

    // event close
    // 关闭BDT协议栈
    close(callback = null);
}

// 状态
BDTStack.STATE = {
    init: 0,
    pinging: 1,
    online: 2,
    closing: 10,
    closed: 11,
};

// 事件列表
BDTStack.EVENT = {
    create: 'create',
    online: 'online',
    close: 'close',
    error: 'error'
};

class BDTAcceptor extends EventEmitter {

    // 开始监听
    listen();

    // 结束监听
    close();

    // 状态
    get state();

    // vport
    get vport();
}

// 状态
BDTAcceptor.STATE = {
    init: 0,
    listening: 1,
    closing: 2,
    closed: 10,
}

// 事件
BDTAcceptor.EVENT = {
    connection: 'connection', // 监听到连接
    close: 'close' // 关闭
}

class BDTConnection extends EventEmitter {

    // 对方peer信息{peerid, vport}
    get remote();

    // 绑定vport，如果不指定vport，会自动分配一个未使用的vport
    bind(vport = 0);

    /*params:{
        peerid:string acceptor's peerid
        vport:number acceport's vport
    }
    */
    // event error
    // event connect
    // 开始连接
    connect(params, callback);

    // 发送数据
    send(buffer);

    // 关闭连接
    // force = true表示要直接关闭，不等对方回复
    close(force = false, callback = null);
}

// 连接状态，类似TCP
BDTConnection.STATE = {
    init: -1,
    closed: 0,
    waitAck: 1,
    waitAckAck: 2,
    break: 3,
    establish: 4,
    finWait1: 5,
    finWait2: 6,
    closing: 7,
    timeWait: 8,
    closeWait: 9,
    lastAck: 10,

    toString(state);
};

// 连接创建方式
BDTConnection.CREATE_FROM = {
    connect: 0,
    acceptor: 1,
};

// 事件
BDTConnection.EVENT = {
    error: 'error', // 错误
    connect: 'connect', // 连通
    close: 'close', // 关闭
    data: 'data', // 收到数据
    drain: 'drain'  // 发送完
};

class SN extends EventEmitter {

    // 停止服务
    stop();
}

// 事件
SN.EVENT = {
    start: 'start',
    stop: 'stop',
}
