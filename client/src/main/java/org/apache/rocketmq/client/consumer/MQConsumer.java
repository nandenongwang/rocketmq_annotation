package org.apache.rocketmq.client.consumer;

import java.util.Set;

import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * 消费者接口
 */
public interface MQConsumer extends MQAdmin {
    /**
     * 回发失败重试消息
     */
    @Deprecated
    void sendMessageBack(MessageExt msg, int delayLevel) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * 指定延时等级发回消息到指定broker组重试
     */
    void sendMessageBack(MessageExt msg, int delayLevel, String brokerName) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

    /**
     * 获取该topic下所有readqueue
     */
    Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException;
}
