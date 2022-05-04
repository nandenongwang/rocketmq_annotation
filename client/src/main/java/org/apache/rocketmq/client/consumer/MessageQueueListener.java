package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Set;

/**
 * 分配消费queue变化监听器
 * litepull型消费者使用该监听器用于重平衡后跟新指定拉取queue并启动或更新拉取任务
 */
public interface MessageQueueListener {
    /**
     * @param topic     message topic
     * @param mqAll     all queues in this message topic
     * @param mqDivided collection of queues,assigned to the current consumer
     */
    void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided);
}
