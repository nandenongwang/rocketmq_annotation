package org.apache.rocketmq.client.hook;

import java.util.List;

import lombok.Data;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 未使用
 */
@Data
public class FilterMessageContext {
    private String consumerGroup;
    private List<MessageExt> msgList;
    private MessageQueue mq;
    private Object arg;
    private boolean unitMode;
}
