package org.apache.rocketmq.client.consumer.listener;

import lombok.Data;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 并发消费上下文
 */
@Data
public class ConsumeConcurrentlyContext {
    private final MessageQueue messageQueue;
    /**
     * 默认0、由broker控制延时等级 一般为重试次数 + 3
     * 也可由业务消费时手动设置上下文延时等级
     * Message consume retry strategy<br>
     * -1,no retry,put into DLQ directly<br>
     * 0,broker control retry frequency<br>
     * >0,client control retry frequency
     */
    private int delayLevelWhenNextConsume = 0;

    /**
     * 确认位置
     * 一批消息中该位置前的消息会被当做成功之后的失败重试
     * 但根据业务方消费返回值统计信息可能不准确 【如返回consume_later 并不会将该位置前消息数量计入成功统计中】
     */
    private int ackIndex = Integer.MAX_VALUE;

    public ConsumeConcurrentlyContext(MessageQueue messageQueue) {
        this.messageQueue = messageQueue;
    }

}
