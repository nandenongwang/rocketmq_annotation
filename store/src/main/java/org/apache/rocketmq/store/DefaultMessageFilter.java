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
package org.apache.rocketmq.store;

import lombok.AllArgsConstructor;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 默认broker端消息过滤器
 */
@Deprecated
@AllArgsConstructor
public class DefaultMessageFilter implements MessageFilter {

    /**
     * 订阅配置
     */
    private SubscriptionData subscriptionData;

    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        //消息没有tag表示可被所有订阅消费
        //消费者没配置订阅表示可消费所有消息
        if (null == tagsCode || null == subscriptionData) {
            return true;
        }

        //classfilter弃用 默认true
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }
        //消费者订阅了所有tags或者订阅tags中包含该消息tag
        return subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)
                || subscriptionData.getCodeSet().contains(tagsCode.intValue());
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        return true;
    }
}
