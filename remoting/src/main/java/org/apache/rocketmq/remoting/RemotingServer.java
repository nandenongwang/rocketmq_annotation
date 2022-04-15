/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;

import java.util.concurrent.ExecutorService;

import org.apache.rocketmq.remoting.common.Pair;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface RemotingServer extends RemotingService {
    /**
     * 注册命令处理器
     */
    void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor);

    /**
     * 注册默认命令处理器
     */
    void registerDefaultProcessor(NettyRequestProcessor processor, ExecutorService executor);

    /**
     * server监听端口
     */
    int localListenPort();

    /**
     * 获取命令的处理器
     */
    Pair<NettyRequestProcessor, ExecutorService> getProcessorPair(int requestCode);

    /**
     * 同步发送命令
     */
    RemotingCommand invokeSync(Channel channel, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 异步发送命令
     */
    void invokeAsync(Channel channel, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * oneway发送命令
     */
    void invokeOneway(Channel channel, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

}
