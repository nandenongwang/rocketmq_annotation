/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.broker.processor;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.RemotingResponseCallback;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

/**
 * 处理producer发送消息
 */
public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

    private List<ConsumeMessageHook> consumeMessageHookList;

    public SendMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = null;
        try {
            response = asyncProcessRequest(ctx, request).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("process SendMessage error, request : " + request.toString(), e);
        }
        return response;
    }

    @Override
    public void asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request, RemotingResponseCallback responseCallback) throws Exception {
        asyncProcessRequest(ctx, request).thenAcceptAsync(responseCallback::callback, this.brokerController.getSendMessageExecutor());
    }

    /**
     * 处理发送消息请求
     */
    public CompletableFuture<RemotingCommand> asyncProcessRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {

        //region 处理重试消息 【消息在consumer端消费失败 消息offset被发送回来进行重试】
        if (request.getCode() == RequestCode.CONSUMER_SEND_MSG_BACK) {
            return this.asyncConsumerSendMsgBack(ctx, request);
        }
        //endregion

        //region 处理正常消息

        //1、解析请求头
        SendMessageRequestHeader requestHeader = parseRequestHeader(request);
        if (requestHeader == null) {
            return CompletableFuture.completedFuture(null);
        }

        //2、执行消息消息发送前置钩子 【仅消息追踪用】
        SendMessageContext mqtraceContext = buildMsgContext(ctx, requestHeader);
        this.executeSendMessageHookBefore(ctx, request, mqtraceContext);

        //3、俺单条消息与批量消息分别处理
        if (requestHeader.isBatch()) {
            return this.asyncSendBatchMessage(ctx, request, mqtraceContext, requestHeader);
        } else {
            return this.asyncSendMessage(ctx, request, mqtraceContext, requestHeader);
        }
        //endregion

    }

    @Override
    public boolean rejectRequest() {
        return this.brokerController.getMessageStore().isOSPageCacheBusy() ||
                this.brokerController.getMessageStore().isTransientStorePoolDeficient();
    }

    /**
     * 处理失败消息 【创建重试queue和延时queue 将消息投到延时queue中】
     */
    private CompletableFuture<RemotingCommand> asyncConsumerSendMsgBack(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ConsumerSendMsgBackRequestHeader requestHeader = (ConsumerSendMsgBackRequestHeader) request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);
        String namespace = NamespaceUtil.getNamespaceFromResource(requestHeader.getGroup());

        //region 消费消息后置钩子
        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {
            ConsumeMessageContext context = buildConsumeMessageContext(namespace, requestHeader, request);
            this.executeConsumeMessageHookAfter(context);
        }
        //endregion

        //region 获取并检查订阅组配置
        SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                    + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return CompletableFuture.completedFuture(response);
        }
        //endregion

        //region 检查broker是否有写权限 & 消费者重试queue数量
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending message is forbidden");
            return CompletableFuture.completedFuture(response);
        }
        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return CompletableFuture.completedFuture(response);
        }
        //endregion

        //region 获取消费组的重试topic newTopic
        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
        int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();
        int topicSysFlag = 0;
        if (requestHeader.isUnitMode()) {
            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
        }
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                newTopic,
                subscriptionGroupConfig.getRetryQueueNums(),
                PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
        if (null == topicConfig) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("topic[" + newTopic + "] not exist");
            return CompletableFuture.completedFuture(response);
        }
        //之前已经存在的重试topic需检查写权限
        if (!PermName.isWriteable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
            return CompletableFuture.completedFuture(response);
        }
        //endregion

        //region 从messagesotre中获取存储原始消息
        MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
        if (null == msgExt) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return CompletableFuture.completedFuture(response);
        }
        //endregion

        //region 处理重试次数 更新延时等级或变更为死信消息
        //设置消息归属topic
        String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (null == retryTopic) {
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        msgExt.setWaitStoreMsgOK(false);

        int delayLevel = requestHeader.getDelayLevel();

        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
        if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
            maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
        }
        //消息重试超过最大重试次数 创建死信topic
        //消息重试小于最大重试次数 加大延时等级
        if (msgExt.getReconsumeTimes() >= maxReconsumeTimes || delayLevel < 0) {
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;

            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic, DLQ_NUMS_PER_GROUP, PermName.PERM_WRITE, 0);
            if (null == topicConfig) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("topic[" + newTopic + "] not exist");
                return CompletableFuture.completedFuture(response);
            }
        } else {
            if (0 == delayLevel) {
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }
            msgExt.setDelayTimeLevel(delayLevel);
        }
        //endregion

        //region 封装内部存储消息
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(newTopic/* 消费组重试topic */);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(msgExt.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);
        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);
        //endregion

        //region 进行消息存储
        CompletableFuture<PutMessageResult> putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
        //endregion

        //region 更新broker统计状态 【重试消息数】
        return putMessageResult.thenApply((r) -> {
            if (r != null) {
                if (r.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                    String backTopic = msgExt.getTopic();
                    String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                    if (correctTopic != null) {
                        backTopic = correctTopic;
                    }
                    this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);
                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);
                    return response;
                }
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(r.getPutMessageStatus().name());
                return response;
            }
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("putMessageResult is null");
            return response;
        });
        //endregion

    }

    /**
     * 异步发送消息
     */
    private CompletableFuture<RemotingCommand> asyncSendMessage(ChannelHandlerContext ctx,
                                                                RemotingCommand request,
                                                                SendMessageContext mqtraceContext,
                                                                SendMessageRequestHeader requestHeader) {
        SendMessageResponseHeader responseHeader;

        //region 预发送
        RemotingCommand response = preSend(ctx, request, requestHeader);
        responseHeader = (SendMessageResponseHeader) response.readCustomHeader();
        if (response.getCode() != -1) {
            return CompletableFuture.completedFuture(response);
        }
        //endregion

        //region 封装内部消息
        byte[] body = request.getBody();
        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        //未设置queueId 随机选择一queue
        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);
        //处理重试&死信
        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
            return CompletableFuture.completedFuture(response);
        }
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(msgInner, MessageConst.PROPERTY_CLUSTER, clusterName);
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));

        //endregion

        CompletableFuture<PutMessageResult> putMessageResult = null;

        boolean transFlag;
        //region 是否是事务消息
        Map<String, String> origProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        transFlag = Boolean.parseBoolean(origProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED));
        //endregion

        if (transFlag) {
            //region 处理事务消息
            //region 检查是否启用事务消息
            if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(
                        "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                                + "] sending transaction message is forbidden");
                return CompletableFuture.completedFuture(response);
            }
            //endregion
            putMessageResult = this.brokerController.getTransactionalMessageService().asyncPrepareMessage(msgInner);
            //endregion
        } else {
            //region 处理非事务消息
            putMessageResult = this.brokerController.getMessageStore().asyncPutMessage(msgInner);
            //endregion
        }
        return handlePutMessageResultFuture(putMessageResult, response, request, msgInner, responseHeader, mqtraceContext, ctx, queueIdInt);
    }

    /**
     * 异步发送批量消息 不支持事务&延迟
     */
    private CompletableFuture<RemotingCommand> asyncSendBatchMessage(ChannelHandlerContext ctx,
                                                                     RemotingCommand request,
                                                                     SendMessageContext mqtraceContext,
                                                                     SendMessageRequestHeader requestHeader) {
        RemotingCommand response = preSend(ctx, request, requestHeader);
        SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.readCustomHeader();

        if (response.getCode() != -1) {
            return CompletableFuture.completedFuture(response);
        }

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = randomQueueId(topicConfig.getWriteQueueNums());
        }

        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark("message topic length too long " + requestHeader.getTopic().length());
            return CompletableFuture.completedFuture(response);
        }

        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(requestHeader.getTopic());
        messageExtBatch.setQueueId(queueIdInt);

        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        messageExtBatch.setSysFlag(sysFlag);

        messageExtBatch.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(messageExtBatch, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        messageExtBatch.setBody(request.getBody());
        messageExtBatch.setBornTimestamp(requestHeader.getBornTimestamp());
        messageExtBatch.setBornHost(ctx.channel().remoteAddress());
        messageExtBatch.setStoreHost(this.getStoreHost());
        messageExtBatch.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        String clusterName = this.brokerController.getBrokerConfig().getBrokerClusterName();
        MessageAccessor.putProperty(messageExtBatch, MessageConst.PROPERTY_CLUSTER, clusterName);

        CompletableFuture<PutMessageResult> putMessageResult = this.brokerController.getMessageStore().asyncPutMessages(messageExtBatch);
        return handlePutMessageResultFuture(putMessageResult, response, request, messageExtBatch, responseHeader, mqtraceContext, ctx, queueIdInt);
    }

    //region 处理消息存储结果
    private CompletableFuture<RemotingCommand> handlePutMessageResultFuture(CompletableFuture<PutMessageResult> putMessageResult,
                                                                            RemotingCommand response,
                                                                            RemotingCommand request,
                                                                            MessageExt msgInner,
                                                                            SendMessageResponseHeader responseHeader,
                                                                            SendMessageContext sendMessageContext,
                                                                            ChannelHandlerContext ctx,
                                                                            int queueIdInt) {
        return putMessageResult.thenApply((r) ->
                handlePutMessageResult(r, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt)
        );
    }

    private RemotingCommand handlePutMessageResult(PutMessageResult putMessageResult,
                                                   RemotingCommand response,
                                                   RemotingCommand request, MessageExt msg,
                                                   SendMessageResponseHeader responseHeader,
                                                   SendMessageContext sendMessageContext,
                                                   ChannelHandlerContext ctx,
                                                   int queueIdInt) {
        if (putMessageResult == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
            return response;
        }
        boolean sendOK = false;

        switch (putMessageResult.getPutMessageStatus()) {
            //region Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOK = true;
                break;
            //endregion

            //region Failed
            case CREATE_MAPEDFILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
            case PROPERTIES_SIZE_EXCEEDED:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(
                        "the message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark(
                        "service not available now. It may be caused by one of the following reasons: " +
                                "the broker's disk is full [" + diskUtil() + "], messages are put to the slave, message store has been shut down, etc.");
                break;
            case OS_PAGECACHE_BUSY:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
            //endregion
        }

        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        if (sendOK) {

            //更新broker状态统计
            this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(), putMessageResult.getAppendMessageResult().getWroteBytes());
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());

            response.setRemark(null);

            responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            responseHeader.setQueueId(queueIdInt);
            responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

            doResponse(ctx, request, response);

            //region 更新sendMessageContext
            if (hasSendMessageHook()) {
                sendMessageContext.setMsgId(responseHeader.getMsgId());
                sendMessageContext.setQueueId(responseHeader.getQueueId());
                sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

                int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
                int incValue = (int) Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT) * commercialBaseCount;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
            //endregion
            return null;
        } else {
            //region 更新sendMessageContext
            if (hasSendMessageHook()) {
                int wroteSize = request.getBody().length;
                int incValue = (int) Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT);

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
            //endregion

        }
        return response;
    }
    //endregion

    /**
     * 死信消息处理
     * 若重试次数过大 更新消息topic为死信topic
     */
    private boolean handleRetryAndDLQ(SendMessageRequestHeader requestHeader, RemotingCommand response, RemotingCommand request, MessageExt msg, TopicConfig topicConfig) {
        String newTopic = requestHeader.getTopic();
        //重试消息
        if (null != newTopic && newTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            //获取消费组订阅组配置
            String groupName = newTopic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
            SubscriptionGroupConfig subscriptionGroupConfig = this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupName);
            if (null == subscriptionGroupConfig) {
                response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
                response.setRemark("subscription group not exist, " + groupName + " " + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
                return false;
            }
            //从订阅组配置中获取最大重试次数
            int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
            if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
                maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
            }
            int reconsumeTimes = requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes();
            //检验该消息重试次数是否大于订阅组最大重试次数 是则发送至死信队列
            if (reconsumeTimes >= maxReconsumeTimes) {
                newTopic = MixAll.getDLQTopic(groupName);
                int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;
                topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                        DLQ_NUMS_PER_GROUP,
                        PermName.PERM_WRITE, 0
                );
                msg.setTopic(newTopic);
                msg.setQueueId(queueIdInt);
                if (null == topicConfig) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("topic[" + newTopic + "] not exist");
                    return false;
                }
            }
        }

        //region 从topic配置中更新消息中tags过滤类型
        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        msg.setSysFlag(sysFlag);
        //endregion
        return true;
    }

    /**
     * 发送消息前前置准备 & 检查
     */
    private RemotingCommand preSend(ChannelHandlerContext ctx, RemotingCommand request, SendMessageRequestHeader requestHeader) {
        RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        //设置回传请求ID
        response.setOpaque(request.getOpaque());

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());

        //broker是否开启消息追踪
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("Receive SendMessage request command {}", request);

        //region 是否准备好接受消息
        long startTimestamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimestamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimestamp)));
            return response;
        }
        //endregion

        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        return response;
    }

    /**
     * 获取broker地址&监听端口
     */
    public SocketAddress getStoreHost() {
        return storeHost;
    }

    /**
     * 计算commitlog consumequeue index文件所在磁盘使用率
     */
    private String diskUtil() {
        String storePathPhysic = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
        double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

        String storePathLogis = StorePathConfigHelper.getStorePathConsumeQueue(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex = StorePathConfigHelper.getStorePathIndex(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }

    /**
     * 随机选取一小于writeQueueNums的数
     */
    private int randomQueueId(int writeQueueNums) {
        return (this.random.nextInt() % 99999999) % writeQueueNums;
    }

    //region 消息消费钩子

    /**
     * 构建消息消费context 用于执行消息消费钩子
     */
    static private ConsumeMessageContext buildConsumeMessageContext(String namespace, ConsumerSendMsgBackRequestHeader requestHeader, RemotingCommand request) {
        ConsumeMessageContext context = new ConsumeMessageContext();
        context.setNamespace(namespace);
        context.setConsumerGroup(requestHeader.getGroup());
        context.setTopic(requestHeader.getOriginTopic());
        context.setCommercialRcvStats(BrokerStatsManager.StatsType.SEND_BACK);
        context.setCommercialRcvTimes(1);
        context.setCommercialOwner(request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));
        return context;
    }

    /**
     * 是否有消息消费钩子
     */
    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    /**
     * 注册消息消费钩子
     */
    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }

    /**
     * 执行消息消费钩子后置处理
     */
    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }
    //endregion


}
