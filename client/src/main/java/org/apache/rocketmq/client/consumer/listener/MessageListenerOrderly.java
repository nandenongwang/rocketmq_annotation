package org.apache.rocketmq.client.consumer.listener;

import java.util.List;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * 业务方顺序处理消息监听器
 */
public interface MessageListenerOrderly extends MessageListener {
    ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context);
}
