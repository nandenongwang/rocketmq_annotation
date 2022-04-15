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

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface RemotingClient extends RemotingService {
    /**
     * 更新nameserver地址
     */
    void updateNameServerAddressList(final List<String> addrs);

    /**
     * 获取nameserver地址
     */
    List<String> getNameServerAddressList();

    /**
     * 同步发送命令
     */
    RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 异步发送命令
     */
    void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * oneway发送命令
     */
    void invokeOneway(String addr, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 注册命令处理器
     */
    void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor);

    /**
     * 设置回调执行线程池
     */
    void setCallbackExecutor(ExecutorService callbackExecutor);

    /**
     * 获取回调执行线程池
     */
    ExecutorService getCallbackExecutor();

    /**
     * addr对应channel是否可写
     */
    boolean isChannelWritable(String addr);
}
