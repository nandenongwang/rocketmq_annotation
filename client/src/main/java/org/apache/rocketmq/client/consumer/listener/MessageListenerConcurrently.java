package org.apache.rocketmq.client.consumer.listener;

import java.util.List;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 并发处理消息监听器
 */
public interface MessageListenerConcurrently extends MessageListener {
    ConsumeConcurrentlyStatus consumeMessage( List<MessageExt> msgs,  ConsumeConcurrentlyContext context);
}
