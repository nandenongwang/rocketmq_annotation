package org.apache.rocketmq.client.impl.consumer;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.consumer.*;
import org.apache.rocketmq.client.consumer.store.LocalFileOffsetStore;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.FilterMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.common.filter.FilterAPI;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.sysflag.PullSysFlag;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.*;
import java.util.concurrent.*;

public class DefaultLitePullConsumerImpl implements MQConsumerInner {

    private final InternalLogger log = ClientLogger.getLog();

    private final long consumerStartTimestamp = System.currentTimeMillis();

    private final RPCHook rpcHook;

    private final ArrayList<FilterMessageHook> filterMessageHookList = new ArrayList<FilterMessageHook>();

    private volatile ServiceState serviceState = ServiceState.CREATE_JUST;

    protected MQClientInstance mQClientFactory;

    @Getter
    private PullAPIWrapper pullAPIWrapper;
    @Getter
    private OffsetStore offsetStore;

    private final RebalanceImpl rebalanceImpl = new RebalanceLitePullImpl(this);

    private enum SubscriptionType {
        NONE, SUBSCRIBE, ASSIGN
    }

    private static final String NOT_RUNNING_EXCEPTION_MESSAGE = "The consumer not running, please start it first.";

    private static final String SUBSCRIPTION_CONFILCT_EXCEPTION_MESSAGE = "Subscribe and assign are mutually exclusive.";

    /**
     * 订阅类型 指定queue或订阅分配queue
     * the type of subscription
     */
    private SubscriptionType subscriptionType = SubscriptionType.NONE;
    /**
     * 异常时重新调度拉取请求间隔时间
     * Delay some time when exception occur
     */
    @Setter
    private long pullTimeDelayMillsWhenException = 1000;
    /**
     * Flow control interval
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL = 50;
    /**
     * Delay some time when suspend pull service
     */
    private static final long PULL_TIME_DELAY_MILLS_WHEN_PAUSE = 1000;
    @Getter
    private final DefaultLitePullConsumer defaultLitePullConsumer;

    /**
     * 所有消费queue的拉取任务
     */
    private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable = new ConcurrentHashMap<>();

    /**
     * queue拉取任务调度线程池
     */
    private final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    /**
     *
     */
    private final AssignedMessageQueue assignedMessageQueue = new AssignedMessageQueue();

    private final BlockingQueue<ConsumeRequest> consumeRequestCache = new LinkedBlockingQueue<>();


    private final ScheduledExecutorService scheduledExecutorService;

    private final Map<String, TopicMessageQueueChangeListener> topicMessageQueueChangeListenerMap = new HashMap<>();

    private final Map<String, Set<MessageQueue>> messageQueuesForTopic = new HashMap<>();

    private long consumeRequestFlowControlTimes = 0L;

    private long queueFlowControlTimes = 0L;

    private long queueMaxSpanFlowControlTimes = 0L;

    private long nextAutoCommitDeadline = -1L;

    private final MessageQueueLock messageQueueLock = new MessageQueueLock();

    public DefaultLitePullConsumerImpl(final DefaultLitePullConsumer defaultLitePullConsumer, final RPCHook rpcHook) {
        this.defaultLitePullConsumer = defaultLitePullConsumer;
        this.rpcHook = rpcHook;
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
                this.defaultLitePullConsumer.getPullThreadNums(),
                new ThreadFactoryImpl("PullMsgThread-" + this.defaultLitePullConsumer.getConsumerGroup())
        );
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "MonitorMessageQueueChangeThread");
            }
        });
        this.pullTimeDelayMillsWhenException = defaultLitePullConsumer.getPullTimeDelayMillsWhenException();
    }

    private void checkServiceState() {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new IllegalStateException(NOT_RUNNING_EXCEPTION_MESSAGE);
        }
    }

    public void updateNameServerAddr(String newAddresses) {
        this.mQClientFactory.getMQClientAPIImpl().updateNameServerAddressList(newAddresses);
    }

    private synchronized void setSubscriptionType(SubscriptionType type) {
        if (this.subscriptionType == SubscriptionType.NONE) {
            this.subscriptionType = type;
        } else if (this.subscriptionType != type) {
            throw new IllegalStateException(SUBSCRIPTION_CONFILCT_EXCEPTION_MESSAGE);
        }
    }

    private void updateAssignedMessageQueue(String topic, Set<MessageQueue> assignedMessageQueue) {
        this.assignedMessageQueue.updateAssignedMessageQueue(topic, assignedMessageQueue);
    }

    private void updatePullTask(String topic, Set<MessageQueue> mqNewSet) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                if (!mqNewSet.contains(next.getKey())) {
                    next.getValue().setCancelled(true);
                    it.remove();
                }
            }
        }
        startPullTask(mqNewSet);
    }

    /**
     * 订阅模式下分配消费queue变化监听器
     */
    class MessageQueueListenerImpl implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            MessageModel messageModel = defaultLitePullConsumer.getMessageModel();
            switch (messageModel) {
                case BROADCASTING:
                    updateAssignedMessageQueue(topic, mqAll);
                    updatePullTask(topic, mqAll);
                    break;
                case CLUSTERING:
                    updateAssignedMessageQueue(topic, mqDivided);
                    updatePullTask(topic, mqDivided);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 关闭消费者
     */
    public synchronized void shutdown() {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                persistConsumerOffset();
                this.mQClientFactory.unregisterConsumer(this.defaultLitePullConsumer.getConsumerGroup());
                scheduledThreadPoolExecutor.shutdown();
                scheduledExecutorService.shutdown();
                this.mQClientFactory.shutdown();
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                log.info("the consumer [{}] shutdown OK", this.defaultLitePullConsumer.getConsumerGroup());
                break;
            default:
                break;
        }
    }

    /**
     * 消费者是否处于运行中
     */
    public synchronized boolean isRunning() {
        return this.serviceState == ServiceState.RUNNING;
    }

    public synchronized void start() throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;

                this.checkConfig();

                if (this.defaultLitePullConsumer.getMessageModel() == MessageModel.CLUSTERING) {
                    this.defaultLitePullConsumer.changeInstanceNameToPID();
                }

                initMQClientFactory();

                initRebalanceImpl();

                initPullAPIWrapper();

                initOffsetStore();

                mQClientFactory.start();

                startScheduleTask();

                this.serviceState = ServiceState.RUNNING;

                log.info("the consumer [{}] start OK", this.defaultLitePullConsumer.getConsumerGroup());

                operateAfterRunning();

                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The PullConsumer service state not OK, maybe started once, "
                        + this.serviceState
                        + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                        null);
            default:
                break;
        }
    }

    private void initMQClientFactory() throws MQClientException {
        this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultLitePullConsumer, this.rpcHook);
        boolean registerOK = mQClientFactory.registerConsumer(this.defaultLitePullConsumer.getConsumerGroup(), this);
        if (!registerOK) {
            this.serviceState = ServiceState.CREATE_JUST;

            throw new MQClientException("The consumer group[" + this.defaultLitePullConsumer.getConsumerGroup()
                    + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                    null);
        }
    }

    private void initRebalanceImpl() {
        this.rebalanceImpl.setConsumerGroup(this.defaultLitePullConsumer.getConsumerGroup());
        this.rebalanceImpl.setMessageModel(this.defaultLitePullConsumer.getMessageModel());
        this.rebalanceImpl.setAllocateMessageQueueStrategy(this.defaultLitePullConsumer.getAllocateMessageQueueStrategy());
        this.rebalanceImpl.setMQClientFactory(this.mQClientFactory);
    }

    private void initPullAPIWrapper() {
        this.pullAPIWrapper = new PullAPIWrapper(
                mQClientFactory,
                this.defaultLitePullConsumer.getConsumerGroup(), isUnitMode());
        this.pullAPIWrapper.registerFilterMessageHook(filterMessageHookList);
    }

    private void initOffsetStore() throws MQClientException {
        if (this.defaultLitePullConsumer.getOffsetStore() != null) {
            this.offsetStore = this.defaultLitePullConsumer.getOffsetStore();
        } else {
            switch (this.defaultLitePullConsumer.getMessageModel()) {
                case BROADCASTING:
                    this.offsetStore = new LocalFileOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                case CLUSTERING:
                    this.offsetStore = new RemoteBrokerOffsetStore(this.mQClientFactory, this.defaultLitePullConsumer.getConsumerGroup());
                    break;
                default:
                    break;
            }
            this.defaultLitePullConsumer.setOffsetStore(this.offsetStore);
        }
        this.offsetStore.load();
    }

    private void startScheduleTask() {
        scheduledExecutorService.scheduleAtFixedRate(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            fetchTopicMessageQueuesAndCompare();
                        } catch (Exception e) {
                            log.error("ScheduledTask fetchMessageQueuesAndCompare exception", e);
                        }
                    }
                }, 1000 * 10, this.getDefaultLitePullConsumer().getTopicMetadataCheckIntervalMillis(), TimeUnit.MILLISECONDS);
    }

    private void operateAfterRunning() throws MQClientException {
        // If subscribe function invoke before start function, then update topic subscribe info after initialization.
        if (subscriptionType == SubscriptionType.SUBSCRIBE) {
            updateTopicSubscribeInfoWhenSubscriptionChanged();
        }
        // If assign function invoke before start function, then update pull task after initialization.
        if (subscriptionType == SubscriptionType.ASSIGN) {
            updateAssignPullTask(assignedMessageQueue.messageQueues());
        }

        for (String topic : topicMessageQueueChangeListenerMap.keySet()) {
            Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
            messageQueuesForTopic.put(topic, messageQueues);
        }
        this.mQClientFactory.checkClientInBroker();
    }

    private void checkConfig() throws MQClientException {
        // Check consumerGroup
        Validators.checkGroup(this.defaultLitePullConsumer.getConsumerGroup());

        // Check consumerGroup name is not equal default consumer group name.
        if (this.defaultLitePullConsumer.getConsumerGroup().equals(MixAll.DEFAULT_CONSUMER_GROUP)) {
            throw new MQClientException(
                    "consumerGroup can not equal "
                            + MixAll.DEFAULT_CONSUMER_GROUP
                            + ", please specify another one."
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // Check messageModel is not null.
        if (null == this.defaultLitePullConsumer.getMessageModel()) {
            throw new MQClientException(
                    "messageModel is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        // Check allocateMessageQueueStrategy is not null
        if (null == this.defaultLitePullConsumer.getAllocateMessageQueueStrategy()) {
            throw new MQClientException(
                    "allocateMessageQueueStrategy is null"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }

        if (this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() < this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis()) {
            throw new MQClientException(
                    "Long polling mode, the consumer consumerTimeoutMillisWhenSuspend must greater than brokerSuspendMaxTimeMillis"
                            + FAQUrl.suggestTodo(FAQUrl.CLIENT_PARAMETER_CHECK_URL),
                    null);
        }
    }

    private void startPullTask(Collection<MessageQueue> mqSet) {
        for (MessageQueue messageQueue : mqSet) {
            if (!this.taskTable.containsKey(messageQueue)) {
                PullTaskImpl pullTask = new PullTaskImpl(messageQueue);
                this.taskTable.put(messageQueue, pullTask);
                this.scheduledThreadPoolExecutor.schedule(pullTask, 0, TimeUnit.MILLISECONDS);
            }
        }
    }

    /**
     * 更新并启动新指定消费queue拉取任务
     */
    private void updateAssignPullTask(Collection<MessageQueue> mqNewSet) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (!mqNewSet.contains(next.getKey())) {
                next.getValue().setCancelled(true);
                it.remove();
            }
        }

        startPullTask(mqNewSet);
    }

    private void updateTopicSubscribeInfoWhenSubscriptionChanged() {
        Map<String, SubscriptionData> subTable = rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            for (final Map.Entry<String, SubscriptionData> entry : subTable.entrySet()) {
                final String topic = entry.getKey();
                this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            }
        }
    }

    /**
     * 订阅topic消息
     */
    public synchronized void subscribe(String topic, String subExpression) throws MQClientException {
        try {
            if (topic == null || topic.equals("")) {
                throw new IllegalArgumentException("Topic can not be null or empty.");
            }
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            SubscriptionData subscriptionData = FilterAPI.buildSubscriptionData(defaultLitePullConsumer.getConsumerGroup(), topic, subExpression);
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            if (serviceState == ServiceState.RUNNING) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }
    }

    public synchronized void subscribe(String topic, MessageSelector messageSelector) throws MQClientException {
        try {
            if (topic == null || topic.equals("")) {
                throw new IllegalArgumentException("Topic can not be null or empty.");
            }
            setSubscriptionType(SubscriptionType.SUBSCRIBE);
            if (messageSelector == null) {
                subscribe(topic, SubscriptionData.SUB_ALL);
                return;
            }
            SubscriptionData subscriptionData = FilterAPI.build(topic,
                    messageSelector.getExpression(), messageSelector.getExpressionType());
            this.rebalanceImpl.getSubscriptionInner().put(topic, subscriptionData);
            this.defaultLitePullConsumer.setMessageQueueListener(new MessageQueueListenerImpl());
            assignedMessageQueue.setRebalanceImpl(this.rebalanceImpl);
            if (serviceState == ServiceState.RUNNING) {
                this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();
                updateTopicSubscribeInfoWhenSubscriptionChanged();
            }
        } catch (Exception e) {
            throw new MQClientException("subscribe exception", e);
        }
    }

    public synchronized void unsubscribe(final String topic) {
        this.rebalanceImpl.getSubscriptionInner().remove(topic);
        removePullTaskCallback(topic);
        assignedMessageQueue.removeAssignedMessageQueue(topic);
    }

    public synchronized void assign(Collection<MessageQueue> messageQueues) {
        if (messageQueues == null || messageQueues.isEmpty()) {
            throw new IllegalArgumentException("Message queues can not be null or empty.");
        }
        setSubscriptionType(SubscriptionType.ASSIGN);
        assignedMessageQueue.updateAssignedMessageQueue(messageQueues);
        if (serviceState == ServiceState.RUNNING) {
            updateAssignPullTask(messageQueues);
        }
    }

    //向offset store更新消费进度 并重置下次自动提交deadline
    private void maybeAutoCommit() {
        long now = System.currentTimeMillis();
        if (now >= nextAutoCommitDeadline) {
            commitAll();
            nextAutoCommitDeadline = now + defaultLitePullConsumer.getAutoCommitIntervalMillis();
        }
    }

    /**
     * 业务侧poll消息消费
     */
    public synchronized List<MessageExt> poll(long timeout) {
        try {
            checkServiceState();
            if (timeout < 0) {
                throw new IllegalArgumentException("Timeout must not be negative");
            }

            if (defaultLitePullConsumer.isAutoCommit()) {
                maybeAutoCommit();
            }
            long endTime = System.currentTimeMillis() + timeout;

            ConsumeRequest consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);

            //还未超时、再次poll消息知道poll超时
            if (endTime - System.currentTimeMillis() > 0) {
                while (consumeRequest != null && consumeRequest.getProcessQueue().isDropped()) {
                    consumeRequest = consumeRequestCache.poll(endTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
                    if (endTime - System.currentTimeMillis() <= 0) {
                        break;
                    }
                }
            }

            if (consumeRequest != null && !consumeRequest.getProcessQueue().isDropped()) {
                List<MessageExt> messages = consumeRequest.getMessageExts();
                long offset = consumeRequest.getProcessQueue().removeMessage(messages);
                assignedMessageQueue.updateConsumeOffset(consumeRequest.getMessageQueue(), offset);
                //If namespace not null , reset Topic without namespace.
                this.resetTopic(messages);
                return messages;
            }
        } catch (InterruptedException ignore) {

        }

        return Collections.emptyList();
    }

    public void pause(Collection<MessageQueue> messageQueues) {
        assignedMessageQueue.pause(messageQueues);
    }

    public void resume(Collection<MessageQueue> messageQueues) {
        assignedMessageQueue.resume(messageQueues);
    }

    /**
     * 手动指定queue查询起始位置
     */
    public synchronized void seek(MessageQueue messageQueue, long offset) throws MQClientException {

        //region 查找的queue必须是指定拉取的queue
        if (!assignedMessageQueue.messageQueues().contains(messageQueue)) {
            if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                throw new MQClientException("The message queue is not in assigned list, may be rebalancing, message queue: " + messageQueue, null);
            } else {
                throw new MQClientException("The message queue is not in assigned list, message queue: " + messageQueue, null);
            }
        }
        //endregion

        //region 从broker获取该queue offset范围、检查查询offset是否有效
        long minOffset = minOffset(messageQueue);
        long maxOffset = maxOffset(messageQueue);
        if (offset < minOffset || offset > maxOffset) {
            throw new MQClientException("Seek offset illegal, seek offset = " + offset + ", min offset = " + minOffset + ", max offset = " + maxOffset, null);
        }
        //endregion

        //region 设置查询位置、并清空处理queue中已pull到的消息缓存、待下次从seek处重新拉取
        final Object objLock = messageQueueLock.fetchLockObject(messageQueue);
        synchronized (objLock) {
            assignedMessageQueue.setSeekOffset(messageQueue, offset);
            clearMessageQueueInCache(messageQueue);
        }
        //endregion
    }

    /**
     * 指定从queue最小offset开始拉取
     */
    public void seekToBegin(MessageQueue messageQueue) throws MQClientException {
        long begin = minOffset(messageQueue);
        this.seek(messageQueue, begin);
    }

    /**
     * 指定从queue最大offset开始拉取
     */
    public void seekToEnd(MessageQueue messageQueue) throws MQClientException {
        long end = maxOffset(messageQueue);
        this.seek(messageQueue, end);
    }

    /**
     * 查询queue最大offset
     */
    private long maxOffset(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(messageQueue);
    }

    /**
     * 查询queue最小offset
     */
    private long minOffset(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().minOffset(messageQueue);
    }

    /**
     *
     */
    private void removePullTaskCallback(final String topic) {
        removePullTask(topic);
    }

    /**
     *
     */
    private void removePullTask(final String topic) {
        Iterator<Map.Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                next.getValue().setCancelled(true);
                it.remove();
            }
        }
    }

    public synchronized void commitAll() {
        try {
            for (MessageQueue messageQueue : assignedMessageQueue.messageQueues()) {
                long consumerOffset = assignedMessageQueue.getConsumerOffset(messageQueue);
                if (consumerOffset != -1) {
                    ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
                    if (processQueue != null && !processQueue.isDropped()) {
                        updateConsumeOffset(messageQueue, consumerOffset);
                    }
                }
            }
            if (defaultLitePullConsumer.getMessageModel() == MessageModel.BROADCASTING) {
                offsetStore.persistAll(assignedMessageQueue.messageQueues());
            }
        } catch (Exception e) {
            log.error("An error occurred when update consume offset Automatically.");
        }
    }

    private void updatePullOffset(MessageQueue messageQueue, long nextPullOffset) {
        if (assignedMessageQueue.getSeekOffset(messageQueue) == -1) {
            assignedMessageQueue.updatePullOffset(messageQueue, nextPullOffset);
        }
    }

    private void submitConsumeRequest(ConsumeRequest consumeRequest) {
        try {
            consumeRequestCache.put(consumeRequest);
        } catch (InterruptedException e) {
            log.error("Submit consumeRequest error", e);
        }
    }

    private long fetchConsumeOffset(MessageQueue messageQueue) {
        checkServiceState();
        return this.rebalanceImpl.computePullFromWhere(messageQueue);
    }

    public long committed(MessageQueue messageQueue) throws MQClientException {
        checkServiceState();
        long offset = this.offsetStore.readOffset(messageQueue, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
        if (offset == -2) {
            throw new MQClientException("Fetch consume offset from broker exception", null);
        }
        return offset;
    }

    private void clearMessageQueueInCache(MessageQueue messageQueue) {
        ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);
        if (processQueue != null) {
            processQueue.clear();
        }
        consumeRequestCache.removeIf(consumeRequest -> consumeRequest.getMessageQueue().equals(messageQueue));
    }

    /**
     * 获取指定queue下次拉取的offset 【seek、pull、rebalance】
     */
    private long nextPullOffset(MessageQueue messageQueue) {
        long offset = -1;
        long seekOffset = assignedMessageQueue.getSeekOffset(messageQueue);
        if (seekOffset != -1) {
            offset = seekOffset;
            assignedMessageQueue.updateConsumeOffset(messageQueue, offset);
            assignedMessageQueue.setSeekOffset(messageQueue, -1);
        } else {
            offset = assignedMessageQueue.getPullOffset(messageQueue);
            if (offset == -1) {
                offset = fetchConsumeOffset(messageQueue);
            }
        }
        return offset;
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        checkServiceState();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    /**
     * 消息拉取任务
     */
    @Data
    public class PullTaskImpl implements Runnable {

        /**
         * 正在拉取的queue
         */
        private final MessageQueue messageQueue;

        /**
         * 该任务是否被取消
         */
        private volatile boolean cancelled = false;

        public PullTaskImpl(final MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        @Override
        public void run() {

            if (!this.isCancelled()) {

                //region 当前拉取的queue暂停中 延迟1重新调度
                if (assignedMessageQueue.isPaused(messageQueue)) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_PAUSE, TimeUnit.MILLISECONDS);
                    log.debug("Message Queue: {} has been paused!", messageQueue);
                    return;
                }
                //endregion

                ProcessQueue processQueue = assignedMessageQueue.getProcessQueue(messageQueue);

                //region 前置检查 【处理queue是否dropped、各种流量控制】
                if (null == processQueue || processQueue.isDropped()) {
                    log.info("The message queue not be able to poll, because it's dropped. group={}, messageQueue={}", defaultLitePullConsumer.getConsumerGroup(), this.messageQueue);
                    return;
                }

                if (consumeRequestCache.size() * defaultLitePullConsumer.getPullBatchSize() > defaultLitePullConsumer.getPullThresholdForAll()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((consumeRequestFlowControlTimes++ % 1000) == 0) {
                        log.warn("The consume request count exceeds threshold {}, so do flow control, consume request count={}, flowControlTimes={}", consumeRequestCache.size(), consumeRequestFlowControlTimes);
                    }
                    return;
                }

                long cachedMessageCount = processQueue.getMsgCount().get();
                long cachedMessageSizeInMiB = processQueue.getMsgSize().get() / (1024 * 1024);

                if (cachedMessageCount > defaultLitePullConsumer.getPullThresholdForQueue()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                                "The cached message count exceeds the threshold {}, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                                defaultLitePullConsumer.getPullThresholdForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                    }
                    return;
                }

                if (cachedMessageSizeInMiB > defaultLitePullConsumer.getPullThresholdSizeForQueue()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                                "The cached message size exceeds the threshold {} MiB, so do flow control, minOffset={}, maxOffset={}, count={}, size={} MiB, flowControlTimes={}",
                                defaultLitePullConsumer.getPullThresholdSizeForQueue(), processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), cachedMessageCount, cachedMessageSizeInMiB, queueFlowControlTimes);
                    }
                    return;
                }

                if (processQueue.getMaxSpan() > defaultLitePullConsumer.getConsumeMaxSpan()) {
                    scheduledThreadPoolExecutor.schedule(this, PULL_TIME_DELAY_MILLS_WHEN_FLOW_CONTROL, TimeUnit.MILLISECONDS);
                    if ((queueMaxSpanFlowControlTimes++ % 1000) == 0) {
                        log.warn(
                                "The queue's messages, span too long, so do flow control, minOffset={}, maxOffset={}, maxSpan={}, flowControlTimes={}",
                                processQueue.getMsgTreeMap().firstKey(), processQueue.getMsgTreeMap().lastKey(), processQueue.getMaxSpan(), queueMaxSpanFlowControlTimes);
                    }
                    return;
                }
                //endregion

                //计算本次拉取开始位置
                long offset = nextPullOffset(messageQueue);
                long pullDelayTimeMills = 0;
                try {
                    //region 确定订阅过滤配置
                    SubscriptionData subscriptionData;
                    if (subscriptionType == SubscriptionType.SUBSCRIBE) {
                        String topic = this.messageQueue.getTopic();
                        subscriptionData = rebalanceImpl.getSubscriptionInner().get(topic);
                    } else {
                        String topic = this.messageQueue.getTopic();
                        subscriptionData = FilterAPI.buildSubscriptionData(defaultLitePullConsumer.getConsumerGroup(),
                                topic, SubscriptionData.SUB_ALL);
                    }
                    //endregion

                    //拉取消息
                    PullResult pullResult = pull(messageQueue, subscriptionData, offset, defaultLitePullConsumer.getPullBatchSize());

                    switch (pullResult.getPullStatus()) {
                        case FOUND:
                            final Object objLock = messageQueueLock.fetchLockObject(messageQueue);
                            synchronized (objLock) {
                                if (pullResult.getMsgFoundList() != null && !pullResult.getMsgFoundList().isEmpty() && assignedMessageQueue.getSeekOffset(messageQueue) == -1) {
                                    //将拉取到消息添加到处理queue消息缓存中
                                    processQueue.putMessage(pullResult.getMsgFoundList());
                                    //提交消费请求、唤醒业务侧poll阻塞线程处理消息
                                    submitConsumeRequest(new ConsumeRequest(pullResult.getMsgFoundList(), messageQueue, processQueue));
                                }
                            }
                            break;
                        case OFFSET_ILLEGAL:
                            log.warn("The pull request offset illegal, {}", pullResult.toString());
                            break;
                        default:
                            break;
                    }
                    //更新下次拉取位置
                    updatePullOffset(messageQueue, pullResult.getNextBeginOffset());
                } catch (Throwable e) {
                    pullDelayTimeMills = pullTimeDelayMillsWhenException;
                    log.error("An error occurred in pull message process.", e);
                }

                //region 继续调度该请求拉取消息
                if (!this.isCancelled()) {
                    scheduledThreadPoolExecutor.schedule(this, pullDelayTimeMills, TimeUnit.MILLISECONDS);
                } else {
                    log.warn("The Pull Task is cancelled after doPullTask, {}", messageQueue);
                }
                //endregion

            }
        }
    }

    /**
     *
     */
    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return pull(mq, subscriptionData, offset, maxNums, this.defaultLitePullConsumer.getConsumerPullTimeoutMillis());
    }

    /**
     *
     */
    private PullResult pull(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.pullSyncImpl(mq, subscriptionData, offset, maxNums, true, timeout);
    }

    /**
     *
     */
    private PullResult pullSyncImpl(MessageQueue mq, SubscriptionData subscriptionData, long offset, int maxNums, boolean block, long timeout)
            throws MQClientException, RemotingException, MQBrokerException, InterruptedException {

        if (null == mq) {
            throw new MQClientException("mq is null", null);
        }

        if (offset < 0) {
            throw new MQClientException("offset < 0", null);
        }

        if (maxNums <= 0) {
            throw new MQClientException("maxNums <= 0", null);
        }

        int sysFlag = PullSysFlag.buildSysFlag(false, block, true, false, true);

        long timeoutMillis = block ? this.defaultLitePullConsumer.getConsumerTimeoutMillisWhenSuspend() : timeout;

        boolean isTagType = ExpressionType.isTagType(subscriptionData.getExpressionType());
        PullResult pullResult = this.pullAPIWrapper.pullKernelImpl(
                mq,
                subscriptionData.getSubString(),
                subscriptionData.getExpressionType(),
                isTagType ? 0L : subscriptionData.getSubVersion(),
                offset,
                maxNums,
                sysFlag,
                0,
                this.defaultLitePullConsumer.getBrokerSuspendMaxTimeMillis(),
                timeoutMillis,
                CommunicationMode.SYNC,
                null
        );
        this.pullAPIWrapper.processPullResult(mq, pullResult, subscriptionData);
        return pullResult;
    }

    /**
     *
     */
    private void resetTopic(List<MessageExt> msgList) {
        if (null == msgList || msgList.size() == 0) {
            return;
        }

        //If namespace not null , reset Topic without namespace.
        for (MessageExt messageExt : msgList) {
            if (null != this.defaultLitePullConsumer.getNamespace()) {
                messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.defaultLitePullConsumer.getNamespace()));
            }
        }

    }

    /**
     *
     */
    public void updateConsumeOffset(MessageQueue mq, long offset) {
        checkServiceState();
        this.offsetStore.updateOffset(mq, offset, false);
    }

    @Override
    public String groupName() {
        return this.defaultLitePullConsumer.getConsumerGroup();
    }

    @Override
    public MessageModel messageModel() {
        return this.defaultLitePullConsumer.getMessageModel();
    }

    @Override
    public ConsumeType consumeType() {
        return ConsumeType.CONSUME_ACTIVELY;
    }

    @Override
    public ConsumeFromWhere consumeFromWhere() {
        return ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET;
    }

    @Override
    public Set<SubscriptionData> subscriptions() {

        return new HashSet<>(this.rebalanceImpl.getSubscriptionInner().values());
    }

    @Override
    public void doRebalance() {
        if (this.rebalanceImpl != null) {
            this.rebalanceImpl.doRebalance(false);
        }
    }

    /**
     * 持久化所有queue的消费进度
     */
    @Override
    public void persistConsumerOffset() {
        try {
            checkServiceState();
            Set<MessageQueue> mqs = new HashSet<>();
            if (this.subscriptionType == SubscriptionType.SUBSCRIBE) {
                Set<MessageQueue> allocateMq = this.rebalanceImpl.getProcessQueueTable().keySet();
                mqs.addAll(allocateMq);
            } else if (this.subscriptionType == SubscriptionType.ASSIGN) {
                Set<MessageQueue> assignedMessageQueue = this.assignedMessageQueue.getAssignedMessageQueues();
                mqs.addAll(assignedMessageQueue);
            }
            this.offsetStore.persistAll(mqs);
        } catch (Exception e) {
            log.error("Persist consumer offset error for group: {} ", this.defaultLitePullConsumer.getConsumerGroup(), e);
        }
    }

    @Override
    public void updateTopicSubscribeInfo(String topic, Set<MessageQueue> info) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                this.rebalanceImpl.getTopicSubscribeInfoTable().put(topic, info);
            }
        }
    }

    @Override
    public boolean isSubscribeTopicNeedUpdate(String topic) {
        Map<String, SubscriptionData> subTable = this.rebalanceImpl.getSubscriptionInner();
        if (subTable != null) {
            if (subTable.containsKey(topic)) {
                return !this.rebalanceImpl.topicSubscribeInfoTable.containsKey(topic);
            }
        }

        return false;
    }


    @Override
    public boolean isUnitMode() {
        return this.defaultLitePullConsumer.isUnitMode();
    }

    /**
     *
     */
    @Override
    public ConsumerRunningInfo consumerRunningInfo() {
        ConsumerRunningInfo info = new ConsumerRunningInfo();

        Properties prop = MixAll.object2Properties(this.defaultLitePullConsumer);
        prop.put(ConsumerRunningInfo.PROP_CONSUMER_START_TIMESTAMP, String.valueOf(this.consumerStartTimestamp));
        info.setProperties(prop);

        info.getSubscriptionSet().addAll(this.subscriptions());
        return info;
    }

    /**
     *
     */
    private void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException,
            MQBrokerException, InterruptedException, MQClientException {
        this.offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
    }

    /**
     *
     */
    public Set<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
        checkServiceState();
        Set<MessageQueue> result = this.mQClientFactory.getMQAdminImpl().fetchSubscribeMessageQueues(topic);
        return parseMessageQueues(result);
    }

    /**
     *
     */
    private synchronized void fetchTopicMessageQueuesAndCompare() throws MQClientException {
        for (Map.Entry<String, TopicMessageQueueChangeListener> entry : topicMessageQueueChangeListenerMap.entrySet()) {
            String topic = entry.getKey();
            TopicMessageQueueChangeListener topicMessageQueueChangeListener = entry.getValue();
            Set<MessageQueue> oldMessageQueues = messageQueuesForTopic.get(topic);
            Set<MessageQueue> newMessageQueues = fetchMessageQueues(topic);
            boolean isChanged = !isSetEqual(newMessageQueues, oldMessageQueues);
            if (isChanged) {
                messageQueuesForTopic.put(topic, newMessageQueues);
                if (topicMessageQueueChangeListener != null) {
                    topicMessageQueueChangeListener.onChanged(topic, newMessageQueues);
                }
            }
        }
    }

    /**
     *
     */
    private boolean isSetEqual(Set<MessageQueue> set1, Set<MessageQueue> set2) {
        if (set1 == null && set2 == null) {
            return true;
        }

        if (set1 == null || set2 == null || set1.size() != set2.size()
                || set1.size() == 0 || set2.size() == 0) {
            return false;
        }

        Iterator<MessageQueue> iter = set2.iterator();
        boolean isEqual = true;
        while (iter.hasNext()) {
            if (!set1.contains(iter.next())) {
                isEqual = false;
            }
        }
        return isEqual;
    }

    /**
     *
     */
    public synchronized void registerTopicMessageQueueChangeListener(String topic, TopicMessageQueueChangeListener listener) throws MQClientException {
        if (topic == null || listener == null) {
            throw new MQClientException("Topic or listener is null", null);
        }
        if (topicMessageQueueChangeListenerMap.containsKey(topic)) {
            log.warn("Topic {} had been registered, new listener will overwrite the old one", topic);
        }
        topicMessageQueueChangeListenerMap.put(topic, listener);
        if (this.serviceState == ServiceState.RUNNING) {
            Set<MessageQueue> messageQueues = fetchMessageQueues(topic);
            messageQueuesForTopic.put(topic, messageQueues);
        }
    }

    /**
     *
     */
    private Set<MessageQueue> parseMessageQueues(Set<MessageQueue> queueSet) {
        Set<MessageQueue> resultQueues = new HashSet<>();
        for (MessageQueue messageQueue : queueSet) {
            String userTopic = NamespaceUtil.withoutNamespace(messageQueue.getTopic(), this.defaultLitePullConsumer.getNamespace());
            resultQueues.add(new MessageQueue(userTopic, messageQueue.getBrokerName(), messageQueue.getQueueId()));
        }
        return resultQueues;
    }

    /**
     * 消费请求
     */
    @AllArgsConstructor
    @Data
    public static class ConsumeRequest {

        /**
         * 拉取到的消息
         */
        private final List<MessageExt> messageExts;

        /**
         * 拉取queue
         */
        private final MessageQueue messageQueue;

        /**
         * 处理queue
         */
        private final ProcessQueue processQueue;
    }
}
