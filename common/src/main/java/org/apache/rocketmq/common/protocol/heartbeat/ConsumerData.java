package org.apache.rocketmq.common.protocol.heartbeat;

import lombok.Data;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;

import java.util.HashSet;
import java.util.Set;

/**
 * 心跳消费者信息
 */
@Data
public class ConsumerData {

    /**
     * 所属消费组
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
