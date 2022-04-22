package org.apache.rocketmq.client.hook;

import java.util.List;
import java.util.Map;

import lombok.Data;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 消息消费hook 上下文
 */
@Data
public class ConsumeMessageContext {

    /**
     * 消费组
     */
    private String consumerGroup;

    /**
     * 消费的消息
     */
    private List<MessageExt> msgList;

    /**
     * 消费的readqueue
     */
    private MessageQueue mq;

    /**
     * 消费是否成功
     */
    private boolean success;

    /**
     * 业务消费返回状态
     */
    private String status;

    /**
     * hook前置钩子后置钩子传递参数上下文用 【现仅消息追踪使用】
     */
    private Object mqTraceContext;

    /**
     * hook传递参数
     */
    private Map<String, String> props;

    /**
     * 消费者namespace
     */
    private String namespace;
}
