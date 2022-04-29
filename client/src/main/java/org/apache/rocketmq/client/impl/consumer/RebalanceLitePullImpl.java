package org.apache.rocketmq.client.impl.consumer;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.Set;

public class RebalanceLitePullImpl extends RebalanceImpl {

    private final DefaultLitePullConsumerImpl litePullConsumerImpl;

    public RebalanceLitePullImpl(DefaultLitePullConsumerImpl litePullConsumerImpl) {
        this(null, null, null, null, litePullConsumerImpl);
    }

    public RebalanceLitePullImpl(String consumerGroup, MessageModel messageModel,
                                 AllocateMessageQueueStrategy allocateMessageQueueStrategy,
                                 MQClientInstance mQClientFactory, DefaultLitePullConsumerImpl litePullConsumerImpl) {
        super(consumerGroup, messageModel, allocateMessageQueueStrategy, mQClientFactory);
        this.litePullConsumerImpl = litePullConsumerImpl;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        MessageQueueListener messageQueueListener = this.litePullConsumerImpl.getDefaultLitePullConsumer().getMessageQueueListener();
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
        this.litePullConsumerImpl.getOffsetStore().persist(mq);
        this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
        return true;
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public void removeDirtyOffset(final MessageQueue mq) {
        this.litePullConsumerImpl.getOffsetStore().removeOffset(mq);
    }

    /**
     * 计算litepull指定queue首次拉取offset 【与push基本相同】
     */
    @Override
    public long computePullFromWhere(MessageQueue mq) {
        ConsumeFromWhere consumeFromWhere = litePullConsumerImpl.getDefaultLitePullConsumer().getConsumeFromWhere();
        long result = -1;
        switch (consumeFromWhere) {
            //region CONSUME_FROM_LAST_OFFSET策略 取最近消费位置【否则重试取最前、正常取最后】
            case CONSUME_FROM_LAST_OFFSET: {
                long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) { // First start, no offset
                        result = 0L;
                    } else {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }
            //endregion

            //region CONSUME_FROM_FIRST_OFFSET策略 取最近消费位置【否则重试与正常均取最前】
            case CONSUME_FROM_FIRST_OFFSET: {
                long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    result = 0L;
                } else {
                    result = -1;
                }
                break;
            }
            //endregion

            //region CONSUME_FROM_TIMESTAMP策略 取最近消费位置【否则重试取最后、正常取半小时前存储消息位置】
            case CONSUME_FROM_TIMESTAMP: {
                long lastOffset = litePullConsumerImpl.getOffsetStore().readOffset(mq, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                if (lastOffset >= 0) {
                    result = lastOffset;
                } else if (-1 == lastOffset) {
                    if (mq.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                        try {
                            result = this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    } else {
                        try {
                            long timestamp = UtilAll.parseDate(this.litePullConsumerImpl.getDefaultLitePullConsumer().getConsumeTimestamp(),
                                    UtilAll.YYYYMMDDHHMMSS).getTime();
                            result = this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
                        } catch (MQClientException e) {
                            result = -1;
                        }
                    }
                } else {
                    result = -1;
                }
                break;
            }
            //endregion
        }
        return result;
    }

    /**
     * 生产者内部拉取、无需分派pull请求到后台拉取服务中
     */
    @Override
    public void dispatchPullRequest(List<PullRequest> pullRequestList) {
    }

}
