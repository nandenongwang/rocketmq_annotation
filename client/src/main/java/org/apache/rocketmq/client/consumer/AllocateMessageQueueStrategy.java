package org.apache.rocketmq.client.consumer;

import java.util.List;

import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 重平衡readqueue在消费者间分配策略
 */
public interface AllocateMessageQueueStrategy {

    /**
     * 分配算法
     *
     * @param consumerGroup 消费组
     * @param currentCID    当前客户端ID
     * @param mqAll         topic下所有queue
     * @param cidAll        所有消费者客户端ID
     * @return 该客户端消费组下消费者分配到的消费queue
     */
    List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll);

    /**
     * 算法名
     */
    String getName();
}
