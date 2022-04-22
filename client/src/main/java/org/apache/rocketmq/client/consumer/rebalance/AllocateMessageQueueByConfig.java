package org.apache.rocketmq.client.consumer.rebalance;

import java.util.List;

import lombok.Data;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 按指定配置分配 【未使用】
 */
@Data
public class AllocateMessageQueueByConfig implements AllocateMessageQueueStrategy {
    private List<MessageQueue> messageQueueList;

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        return this.messageQueueList;
    }

    @Override
    public String getName() {
        return "CONFIG";
    }
}
