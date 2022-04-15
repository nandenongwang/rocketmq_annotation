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

/**
 * $Id: ConsumerData.java 1835 2013-05-16 02:00:50Z vintagewang@apache.org $
 */
package org.apache.rocketmq.common.protocol.heartbeat;

import java.util.HashSet;
import java.util.Set;

import lombok.Data;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

@Data
public class ConsumerData {
    /**
     * 消费组
     */
    private String groupName;
    /**
     * 消费类型
     */
    private ConsumeType consumeType;
    /**
     * 消费模式
     */
    private MessageModel messageModel;
    /**
     * 初始拉取偏移策略
     */
    private ConsumeFromWhere consumeFromWhere;
    /**
     * 订阅配置
     */
    private Set<SubscriptionData> subscriptionDataSet = new HashSet<>();
    /**
     * unit模式
     */
    @Deprecated
    private boolean unitMode;
}
