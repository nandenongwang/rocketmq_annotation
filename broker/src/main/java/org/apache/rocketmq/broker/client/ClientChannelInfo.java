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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;
import lombok.Data;
import org.apache.rocketmq.remoting.protocol.LanguageCode;

@Data
public class ClientChannelInfo {
    /**
     * 客户端channel
     */
    private final Channel channel;
    /**
     * 客户端实例ID
     */
    private final String clientId;
    /**
     * 客户端语言
     */
    private final LanguageCode language;
    /**
     * 客户端版本
     */
    private final int version;
    /**
     * 最后活跃时间戳
     */
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ClientChannelInfo(Channel channel) {
        this(channel, null, null, 0);
    }

    public ClientChannelInfo(Channel channel, String clientId, LanguageCode language, int version) {
        this.channel = channel;
        this.clientId = clientId;
        this.language = language;
        this.version = version;
    }

}
