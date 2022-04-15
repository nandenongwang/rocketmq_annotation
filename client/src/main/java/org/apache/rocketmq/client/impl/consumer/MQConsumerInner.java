package org.apache.rocketmq.client.impl.consumer;

import java.util.Set;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public interface MQConsumerInner {
    /**
     * 消费组名
     */
    String groupName();

    /**
     * 消费模式 【集群或广播】
     */
    MessageModel messageModel();

    /**
     * 消费者类型 【pull或push】
     */
    ConsumeType consumeType();

    /**
     * 初始消费点获取策略
     */
    ConsumeFromWhere consumeFromWhere();

    /**
     * 订阅配置
     */
    Set<SubscriptionData> subscriptions();

    /**
     * 进行重平衡
     */
    void doRebalance();

    /**
     * 持久化消费进度
     */
    void persistConsumerOffset();

    /**
     * 更新订阅信息
     */
    void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info);

    /**
     * topic 下订阅信息是否需要更新
     */
    boolean isSubscribeTopicNeedUpdate(final String topic);

    /**
     * unit模式
     */
    @Deprecated
    boolean isUnitMode();

    /**
     * 消费者运行状态
     */
    ConsumerRunningInfo consumerRunningInfo();
}
