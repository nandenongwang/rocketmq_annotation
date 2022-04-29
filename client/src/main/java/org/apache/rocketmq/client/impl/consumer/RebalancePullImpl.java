package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.Set;

public class RebalancePullImpl extends RebalanceImpl {
    private final DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

    public RebalancePullImpl(DefaultMQPullConsumerImpl defaultMQPullConsumerImpl) {
        this(null, null, null, null, defaultMQPullConsumerImpl);
    }

    public RebalancePullImpl(String consumerGroup, MessageModel messageModel,
                             AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                             MQClientInstance mQClientFactory, DefaultMQPullConsumerImpl defaultMQPullConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.defaultMQPullConsumerImpl = defaultMQPullConsumerImpl;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        MessageQueueListener messageQueueListener = this.defaultMQPullConsumerImpl.getDefaultMQPullConsumer().getMessageQueueListener();
        if (messageQueueListener != null) {
            try {
                messageQueueListener.messageQueueChanged(topic, mqAll, mqDivided);
            } catch (Throwable e) {
                log.error("messageQueueChanged exception", e);
            }
        }
    }

    @Override
    public boolean removeUnnecessaryMessageQueue(MessageQueue mq, ProcessQueue pq) {
        this.defaultMQPullConsumerImpl.getOffsetStore().persist(mq);
        this.defaultMQPullConsumerImpl.getOffsetStore().removeOffset(mq);
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.defaultMQPullConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    /**
     * 计算起始拉取位置 【pull类型消费者需手动设置起始offset、未设置则都重头拉取】
     */
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        return 0;
    }

    /**
     * pull类型手动拉取消息、无需分派拉取请求
     */
    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
    }
}
