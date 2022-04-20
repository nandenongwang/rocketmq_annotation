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
package org.apache.rocketmq.client.consumer;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 拉取消息结果
 */
@Data
@AllArgsConstructor
public class PullResult {

    /**
     * 拉取状态码
     */
    private final PullStatus pullStatus;
    /**
     * 下次拉取位置
     */
    private final long nextBeginOffset;
    /**
     * 本次拉取消息最小offset
     */
    private final long minOffset;
    /**
     * 本次拉取消息最大offset
     */
    private final long maxOffset;
    /**
     * 本次拉取消息
     */
    private List<MessageExt> msgFoundList;
}
