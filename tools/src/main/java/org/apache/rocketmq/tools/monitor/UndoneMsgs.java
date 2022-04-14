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

package org.apache.rocketmq.tools.monitor;

import lombok.Data;

@Data
public class UndoneMsgs {
    /**
     * 消费组
     */
    private String consumerGroup;
    /**
     * 主题
     */
    private String topic;
    /**
     * 未消费消息总数
     */
    private long undoneMsgsTotal;
    /**
     * 各 consume queue中未消费消息最大值
     */
    private long undoneMsgsSingleMQ;
    /**
     *
     */
    private long undoneMsgsDelayTimeMills;


}
