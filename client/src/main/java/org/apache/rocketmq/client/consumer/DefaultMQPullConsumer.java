package org.apache.rocketmq.client.consumer;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.HashSet;
import java.util.Set;

/**
 * 拉模式消费者
 * 主要功能转发至DefaultMQPullConsumerImpl实现
 * 提供了pull消费者特殊配置参数 & 限制了对业务端功能接口的暴露
 */
@Deprecated
public class DefaultMQPullConsumer extends ClientConfig implements MQPullConsumer {
    @Getter
    protected final transient DefaultMQPullConsumerImpl defaultMQPullConsumerImpl;

    /**
     * 消费组
     */
    @Getter
    @Setter
    private String consumerGroup;

    /**
     * 长轮询下broker端最大挂起时间
     */
    @Getter
    @Setter
    private long brokerSuspendMaxTimeMillis = 1000 * 20;

    /**
     * 长轮询下consumer端挂起等待的超时时间 【必定要大于broker端挂起时间】
     */
    @Getter
    @Setter
    private long consumerTimeoutMillisWhenSuspend = 1000 * 30;

    /**
     * 一次pull请求从建立连接到等待响应的最大时间 【关闭长轮询时】
     */
    @Getter
    @Setter
    private long consumerPullTimeoutMillis = 1000 * 10;

    /**
     * 消费模式 集群或广播
     */
    @Getter
    @Setter
    private MessageModel messageModel = MessageModel.CLUSTERING;

    /**
     * consumequeue分配变更监听器
     */
    @Getter
    @Setter
    private MessageQueueListener messageQueueListener;

    /**
     * 进度管理器
     */
    @Getter
    @Setter
    private OffsetStore offsetStore;

    /**
     * Topic set you want to register
     */
    @Getter
    @Setter
    private Set<String> registerTopics = new HashSet<>();

    /**
     * consumequeue分配策略
     */
    @Getter
    @Setter
    private AllocateMessageQueueStrategy allocateMessageQueueStrategy = new AllocateMessageQueueAveragely();

    @Getter
    @Setter
    private boolean unitMode = false;

    @Getter
    @Setter
    private int maxReconsumeTimes = 16;

    //region 各种构造函数 设置defaultMQPullConsumerImpl 【同时配置namespace、groupname、rpchook等】
    public DefaultMQPullConsumer() {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, null);
    }

    public DefaultMQPullConsumer(final String consumerGroup) {
        this(null, consumerGroup, null);
    }

    public DefaultMQPullConsumer(RPCHook rpcHook) {
        this(null, MixAll.DEFAULT_CONSUMER_GROUP, rpcHook);
    }

    public DefaultMQPullConsumer(final String consumerGroup, RPCHook rpcHook) {
        this(null, consumerGroup, rpcHook);
    }

    public DefaultMQPullConsumer(final String namespace, final String consumerGroup) {
        this(namespace, consumerGroup, null);
    }

    public DefaultMQPullConsumer(final String namespace, final String consumerGroup, RPCHook rpcHook) {
        this.namespace = namespace;
        this.consumerGroup = consumerGroup;
        defaultMQPullConsumerImpl = new DefaultMQPullConsumerImpl(this, rpcHook);
    }
    //endregion

    //region 创建topic
    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, withNamespace(newTopic), queueNum, 0);
    }

    @Deprecated
    @Override
    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.defaultMQPullConsumerImpl.createTopic(key, withNamespace(newTopic), queueNum, topicSysFlag);
    }
    //endregion

    //region offset相关

    /**
     * 搜索consumequeue指定时间戳存储的消息
     */
    @Deprecated
    @Override
    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        return this.defaultMQPullConsumerImpl.searchOffset(queueWithNamespace(mq), timestamp);
    }

    /**
     * 搜索consumequeue最大的offset
     */
    @Deprecated
    @Override
    public long maxOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.maxOffset(queueWithNamespace(mq));
    }

    /**
     * 搜索consumequeue最小的offset
     */
    @Deprecated
    @Override
    public long minOffset(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.minOffset(queueWithNamespace(mq));
    }

    /**
     * 查询consumequeue最早的消息存储时间
     */
    @Deprecated
    @Override
    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        return this.defaultMQPullConsumerImpl.earliestMsgStoreTime(queueWithNamespace(mq));
    }
    //endregion

    //region 查询消息相关
    @Override
    public MessageExt viewMessage(String topic, String uniqKey) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        try {
            MessageDecoder.decodeMessageId(uniqKey);
            return this.viewMessage(uniqKey);
        } catch (Exception ignored) {
        }
        return this.defaultMQPullConsumerImpl.queryMessageByUniqKey(withNamespace(topic), uniqKey);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public MessageExt viewMessage(String offsetMsgId) throws RemotingException, MQBrokerException,
            InterruptedException, MQClientException {
        return this.defaultMQPullConsumerImpl.viewMessage(offsetMsgId);
    }

    /**
     * This method will be removed in a certain version after April 5, 2020, so please do not use this method.
     */
    @Deprecated
    @Override
    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
            throws MQClientException, InterruptedException {
        return this.defaultMQPullConsumerImpl.queryMessage(withNamespace(topic), key, maxNum, begin, end);
    }

    //endregion


    /**
     * 送回消费 【通常用于消费失败消息】
     */
    @Deprecated
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, null);
    }

    /**
     * 送回消费 【通常用于消费失败消息】
     */
    @Deprecated
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName);
    }

    /**
     * 送回消费 【通常用于消费失败消息】
     */
    @Override
    public void sendMessageBack(MessageExt msg, int delayLevel, String brokerName, String consumerGroup)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        msg.setTopic(withNamespace(msg.getTopic()));
        this.defaultMQPullConsumerImpl.sendMessageBack(msg, delayLevel, brokerName, consumerGroup);
    }

    /**
     * 获取topic所有consumequeue
     */
    @Override
    public Set<MessageQueue> fetchSubscribeMessageQueues(String topic) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchSubscribeMessageQueues(withNamespace(topic));
    }

    /**
     * 启动defaultMQPullConsumerImpl
     */
    @Override
    public void start() throws MQClientException {
        this.setConsumerGroup(NamespaceUtil.wrapNamespace(this.getNamespace(), this.consumerGroup));
        this.defaultMQPullConsumerImpl.start();
    }

    /**
     * 关闭 defaultMQPullConsumerImpl
     */
    @Override
    public void shutdown() {
        this.defaultMQPullConsumerImpl.shutdown();
    }

    /**
     * 注册consumequeue监听器
     */
    @Override
    public void registerMessageQueueListener(String topic, MessageQueueListener listener) {
        synchronized (this.registerTopics) {
            this.registerTopics.add(withNamespace(topic));
            if (listener != null) {
                this.messageQueueListener = listener;
            }
        }
    }

    //region 拉取消息 【同步异步、普通长轮询】

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums);
    }

    @Override
    public PullResult pull(MessageQueue mq, String subExpression, long offset, int maxNums, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, timeout);
    }

    @Override
    public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums);
    }

    @Override
    public PullResult pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums, timeout);
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback);
    }

    @Override
    public void pull(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback, timeout);
    }

    @Override
    public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, PullCallback pullCallback) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums, pullCallback);
    }

    @Override
    public void pull(MessageQueue mq, MessageSelector messageSelector, long offset, int maxNums, PullCallback pullCallback, long timeout) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pull(queueWithNamespace(mq), messageSelector, offset, maxNums, pullCallback, timeout);
    }

    @Override
    public PullResult pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums);
    }

    @Override
    public void pullBlockIfNotFound(MessageQueue mq, String subExpression, long offset, int maxNums, PullCallback pullCallback) throws MQClientException, RemotingException, InterruptedException {
        this.defaultMQPullConsumerImpl.pullBlockIfNotFound(queueWithNamespace(mq), subExpression, offset, maxNums, pullCallback);
    }
    //endregion

    /**
     * 业务方手动更新consumequeue消费进度
     */
    @Override
    public void updateConsumeOffset(MessageQueue mq, long offset) throws MQClientException {
        this.defaultMQPullConsumerImpl.updateConsumeOffset(queueWithNamespace(mq), offset);
    }

    /**
     * 抓取consumequeue消费进度 【fromStore是否存储优先】
     */
    @Override
    public long fetchConsumeOffset(MessageQueue mq, boolean fromStore) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchConsumeOffset(queueWithNamespace(mq), fromStore);
    }

    /**
     * 获取分配到消费的consumequeue
     */
    @Override
    public Set<MessageQueue> fetchMessageQueuesInBalance(String topic) throws MQClientException {
        return this.defaultMQPullConsumerImpl.fetchMessageQueuesInBalance(withNamespace(topic));
    }


}
