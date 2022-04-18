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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

@Data
public class ConsumerGroupInfo {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    /**
     * 组名
     */
    private final String groupName;
    /**
     * 订阅配置 key[topic]-value[过滤配置] 一般仅订阅一个topic
     */
    private final ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable = new ConcurrentHashMap<>();
    /**
     * 组内左右消费者客户端实例channel
     */
    private final ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable = new ConcurrentHashMap<>(16);
    /**
     * 消费类型 【push|pull】
     */
    private volatile ConsumeType consumeType;
    /**
     * 消息模式 【集群|广播】
     */
    private volatile MessageModel messageModel;
    /**
     * 起始消费位置策略
     */
    private volatile ConsumeFromWhere consumeFromWhere;
    /**
     * 最后更新时间戳
     */
    private volatile long lastUpdateTimestamp = System.currentTimeMillis();

    public ConsumerGroupInfo(String groupName, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        this.groupName = groupName;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;
    }

    /**
     * 根据clientId获取连接channel信息
     */
    public ClientChannelInfo findChannel(String clientId) {
        for (Entry<Channel, ClientChannelInfo> next : this.channelInfoTable.entrySet()) {
            if (next.getValue().getClientId().equals(clientId)) {
                return next.getValue();
            }
        }

        return null;
    }

    /**
     * 获取所有客户端连接channel
     */
    public List<Channel> getAllChannel() {
        List<Channel> result = new ArrayList<>();

        result.addAll(this.channelInfoTable.keySet());

        return result;
    }

    /**
     * 获取所有客户端id 【一个客户端实例一个消费组下仅只能有一个消费者 此处一个clientId代表一个消费者】
     */
    public List<String> getAllClientId() {
        List<String> result = new ArrayList<>();

        for (Entry<Channel, ClientChannelInfo> entry : this.channelInfoTable.entrySet()) {
            ClientChannelInfo clientChannelInfo = entry.getValue();
            result.add(clientChannelInfo.getClientId());
        }

        return result;
    }

    /**
     * 移除连接channel
     */
    public void unregisterChannel(ClientChannelInfo clientChannelInfo) {
        ClientChannelInfo old = this.channelInfoTable.remove(clientChannelInfo.getChannel());
        if (old != null) {
            log.info("unregister a consumer[{}] from consumerGroupInfo {}", this.groupName, old.toString());
        }
    }

    /**
     * 处理连接关闭事件 【移除连接channel】
     */
    public boolean doChannelCloseEvent(String remoteAddr, Channel channel) {
        ClientChannelInfo info = this.channelInfoTable.remove(channel);
        if (info != null) {
            log.warn("NETTY EVENT: remove not active channel[{}] from ConsumerGroupInfo groupChannelTable, consumer group: {}", info.toString(), groupName);
            return true;
        }

        return false;
    }

    /**
     * 更新消费者channel信息 【消费组配置会根据消费者携带的消费组配置做覆盖】
     */
    public boolean updateChannel(ClientChannelInfo infoNew, ConsumeType consumeType, MessageModel messageModel, ConsumeFromWhere consumeFromWhere) {
        boolean updated = false;
        this.consumeType = consumeType;
        this.messageModel = messageModel;
        this.consumeFromWhere = consumeFromWhere;

        ClientChannelInfo infoOld = this.channelInfoTable.get(infoNew.getChannel());
        if (null == infoOld) {
            ClientChannelInfo prev = this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            if (null == prev) {
                log.info("new consumer connected, group: {} {} {} channel: {}", this.groupName, consumeType, messageModel, infoNew.toString());
                updated = true;
            }

            infoOld = infoNew;
        } else {
            if (!infoOld.getClientId().equals(infoNew.getClientId())) {
                log.error("[BUG] consumer channel exist in broker, but clientId not equal. GROUP: {} OLD: {} NEW: {} ", this.groupName, infoOld.toString(), infoNew.toString());
                this.channelInfoTable.put(infoNew.getChannel(), infoNew);
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();
        infoOld.setLastUpdateTimestamp(this.lastUpdateTimestamp);

        return updated;
    }

    /**
     * 更新订阅配置
     */
    public boolean updateSubscription(Set<SubscriptionData> subList) {
        boolean updated = false;

        for (SubscriptionData sub : subList) {
            SubscriptionData old = this.subscriptionTable.get(sub.getTopic());
            //旧的为空直接存入
            if (old == null) {
                SubscriptionData prev = this.subscriptionTable.putIfAbsent(sub.getTopic(), sub);
                if (null == prev) {
                    updated = true;
                    log.info("subscription changed, add new topic, group: {} {}",
                            this.groupName,
                            sub.toString());
                }
                //旧的不为空需比较版本号
            } else if (sub.getSubVersion() > old.getSubVersion()) {
                if (this.consumeType == ConsumeType.CONSUME_PASSIVELY) {
                    log.info("subscription changed, group: {} OLD: {} NEW: {}", this.groupName, old.toString(), sub.toString());
                }

                this.subscriptionTable.put(sub.getTopic(), sub);
            }
        }
        //删掉旧订阅在新订阅里没有的配置
        Iterator<Entry<String, SubscriptionData>> it = this.subscriptionTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, SubscriptionData> next = it.next();
            String oldTopic = next.getKey();

            boolean exist = false;
            for (SubscriptionData sub : subList) {
                if (sub.getTopic().equals(oldTopic)) {
                    exist = true;
                    break;
                }
            }

            if (!exist) {
                log.warn("subscription changed, group: {} remove topic {} {}", this.groupName, oldTopic, next.getValue().toString());

                it.remove();
                updated = true;
            }
        }

        this.lastUpdateTimestamp = System.currentTimeMillis();

        return updated;
    }

    /**
     * 获取消费组所有订阅的同topic
     */
    public Set<String> getSubscribeTopics() {
        return subscriptionTable.keySet();
    }

    /**
     * 获取消费组topic订阅过滤配置
     */
    public SubscriptionData findSubscriptionData(String topic) {
        return this.subscriptionTable.get(topic);
    }

}
