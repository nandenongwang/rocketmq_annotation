package org.apache.rocketmq.client.consumer;

import java.util.Set;
import org.apache.rocketmq.common.message.MessageQueue;

public interface TopicMessageQueueChangeListener {
    /**
     * This method will be invoked in the condition of queue numbers changed, These scenarios occur when the topic is
     * expanded or shrunk.
     *
     * @param messageQueues
     */
    void onChanged(String topic, Set<MessageQueue> messageQueues);
}
