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

/**
 * channel事件监听器
 * 并处理netty原生channel事件
 */
public interface ChannelEventListener {
    /**
     * 连接
     */
    void onChannelConnect(String remoteAddr, Channel channel);

    /**
     * 关闭
     */
    void onChannelClose(String remoteAddr, Channel channel);

    /**
     * 异常
     */
    void onChannelException(String remoteAddr, Channel channel);

    /**
     * 空闲
     */
    void onChannelIdle(String remoteAddr, Channel channel);
}
