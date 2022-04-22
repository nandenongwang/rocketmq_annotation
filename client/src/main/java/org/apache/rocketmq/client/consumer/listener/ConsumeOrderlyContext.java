package org.apache.rocketmq.client.consumer.listener;

import lombok.Data;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 顺序消费上下文
 */
@Data
public class ConsumeOrderlyContext {

    /**
     * 消费的readqueue
     */
    private final MessageQueue messageQueue;

    /**
     * 是否自动提交
     */
    private boolean autoCommit = true;

    /**
     * 暂停当前readqueue消费间隔时间
     */
    private long suspendCurrentQueueTimeMillis = -1;

    public ConsumeOrderlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

}
