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
package org.apache.rocketmq.client.producer;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.common.message.MessageQueue;

@Data
@NoArgsConstructor
public class SendResult {

    /**
     *
     */
    private SendStatus sendStatus;

    /**
     * producer端生成消息ID
     */
    private String msgId;

    /**
     *
     */
    private MessageQueue messageQueue;

    /**
     *
     */
    private long queueOffset;

    /**
     *
     */
    private String transactionId;

    /**
     * broker端生成消息ID
     */
    private String offsetMsgId;

    /**
     *
     */
    private String regionId;

    /**
     * 开启消息trace
     */
    private boolean traceOn = true;

    public SendResult(SendStatus sendStatus, String msgId, String offsetMsgId, MessageQueue messageQueue, long queueOffset) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.offsetMsgId = offsetMsgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
    }

    public SendResult(SendStatus sendStatus, String msgId, MessageQueue messageQueue, long queueOffset, String transactionId, String offsetMsgId, String regionId) {
        this.sendStatus = sendStatus;
        this.msgId = msgId;
        this.messageQueue = messageQueue;
        this.queueOffset = queueOffset;
        this.transactionId = transactionId;
        this.offsetMsgId = offsetMsgId;
        this.regionId = regionId;
    }

    public static String encoderSendResultToJson(final Object obj) {
        return JSON.toJSONString(obj);
    }

    public static SendResult decoderSendResultFromJson(String json) {
        return JSON.parseObject(json, SendResult.class);
    }

}
