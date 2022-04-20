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
package org.apache.rocketmq.broker.longpolling;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageFilter;

@Data
@AllArgsConstructor
public class PullRequest {
    /**
     * 原始请求命令
     */
    private final RemotingCommand requestCommand;
    /**
     * 客户端channel
     */
    private final Channel clientChannel;
    /**
     * 挂起超时 默认20s
     */
    private final long timeoutMillis;
    /**
     * 挂起开始时间
     */
    private final long suspendTimestamp;
    /**
     * 请求offset
     */
    private final long pullFromThisOffset;
    /**
     * 订阅配置
     */
    private final SubscriptionData subscriptionData;
    /**
     * 消息过滤器
     */
    private final MessageFilter messageFilter;

}
