package org.apache.rocketmq.client.hook;

import java.util.Map;

import lombok.Data;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;

/**
 * 发送消息hook上下文
 */
@Data
public class SendMessageContext {


    /**
     * 发送的消息
     */
    private Message message;

    /**
     * 发送至哪个writequeue
     */
    private MessageQueue mq;

    /**
     * 存储消息broker地址
     */
    private String brokerAddr;

    /**
     * 生产者主机地址
     */
    private String bornHost;


    /**
     * 发送结果
     */
    private SendResult sendResult;

    /**
     * 发送出现的异常
     */
    private Exception exception;

    /**
     * hook前置钩子后置钩子传递参数上下文用 【现仅消息追踪使用】
     */
    private Object mqTraceContext;

    /**
     * 发送的消息的properties
     */
    private Map<String, String> props;

    /**
     * 生产者
     */
    private DefaultMQProducerImpl producer;

    /**
     * 生产组
     */
    private String producerGroup;

    /**
     * 通信模式 同步|异步|oneway
     */
    private CommunicationMode communicationMode;

    /**
     * 消息发送类型
     */
    private MessageType msgType = MessageType.Normal_Msg;

    /**
     * 生产者namespace 默认无
     */
    private String namespace;
}
