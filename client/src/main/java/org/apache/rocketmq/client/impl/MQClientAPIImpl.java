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
package org.apache.rocketmq.client.impl;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.consumer.PullCallback;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.DefaultMQProducerImpl;
import org.apache.rocketmq.client.impl.producer.TopicPublishInfo;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.AclConfig;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.PlainAccessConfig;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.BrokerStatsData;
import org.apache.rocketmq.common.protocol.body.CheckClientRequestBody;
import org.apache.rocketmq.common.protocol.body.ClusterAclVersionInfo;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.common.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.common.protocol.body.QueryCorrectionOffsetBody;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.header.CloneGroupOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.CreateAccessConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteAccessConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetBrokerAclConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetBrokerClusterAclConfigResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumeStatsInBrokerHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerListByGroupResponseBody;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetProducerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetTopicsByClusterRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.PullMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumeQueueRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumerOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.QueryCorrectionOffsetHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResumeCheckHalfMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.UnregisterClientRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateConsumerOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.UpdateGlobalWhiteAddrsConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.ViewBrokerStatsDataRequestHeader;
import org.apache.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.filtersrv.RegisterMessageFilterClassRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetKVListByNamespaceRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.GetRouteInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.PutKVConfigRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerRequestHeader;
import org.apache.rocketmq.common.protocol.header.namesrv.WipeWritePermOfBrokerResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.HeartbeatData;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.netty.ResponseFuture;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;


public class MQClientAPIImpl {

    private final static InternalLogger log = ClientLogger.getLog();

    private static final boolean sendSmartMsg = Boolean.parseBoolean(System.getProperty("org.apache.rocketmq.client.sendSmartMsg", "true"));

    static {
        System.setProperty(RemotingCommand.REMOTING_VERSION_KEY, Integer.toString(MQVersion.CURRENT_VERSION));
    }

    @Getter
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing;
    private final ClientRemotingProcessor clientRemotingProcessor;
    private String nameSrvAddr = null;
    private final ClientConfig clientConfig;

    public MQClientAPIImpl(NettyClientConfig nettyClientConfig, ClientRemotingProcessor clientRemotingProcessor, RPCHook rpcHook, ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        this.topAddressing = new TopAddressing(MixAll.getWSAddr(), clientConfig.getUnitName());
        this.remotingClient = new NettyRemotingClient(nettyClientConfig, null);
        this.clientRemotingProcessor = clientRemotingProcessor;

        this.remotingClient.registerRPCHook(rpcHook);

        this.remotingClient.registerProcessor(RequestCode.CHECK_TRANSACTION_STATE, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.GET_CONSUMER_RUNNING_INFO, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.CONSUME_MESSAGE_DIRECTLY, this.clientRemotingProcessor, null);
        this.remotingClient.registerProcessor(RequestCode.PUSH_REPLY_MESSAGE_TO_CLIENT, this.clientRemotingProcessor, null);
    }

    public List<String> getNameServerAddressList() {
        return this.remotingClient.getNameServerAddressList();
    }

    //region 请求客户端

    /**
     * 拉取nameserver地址列表
     */
    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old=" + this.nameSrvAddr + ", new=" + addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    /**
     * 更新请求客户端nameserver地址列表
     */
    public void updateNameServerAddressList(final String addrs) {
        String[] addrArray = addrs.split(";");
        List<String> list = Arrays.asList(addrArray);
        this.remotingClient.updateNameServerAddressList(list);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
    }
    //endregion

    //region send消息
    public SendResult sendMessage(String addr, String brokerName, Message msg, SendMessageRequestHeader requestHeader, long timeoutMillis,
                                  CommunicationMode communicationMode, SendMessageContext context, DefaultMQProducerImpl producer)
            throws RemotingException, MQBrokerException, InterruptedException {
        return sendMessage(addr, brokerName, msg, requestHeader, timeoutMillis, communicationMode, null, null, null, 0, context, producer);
    }

    public SendResult sendMessage(String addr, String brokerName, Message msg, SendMessageRequestHeader requestHeader,
                                  long timeoutMillis, CommunicationMode communicationMode, SendCallback sendCallback, TopicPublishInfo topicPublishInfo,
                                  MQClientInstance instance, int retryTimesWhenSendFailed, SendMessageContext context, DefaultMQProducerImpl producer)
            throws RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        RemotingCommand request = null;

        //region 设置rpc响应消息和普通消息请求码
        String msgType = msg.getProperty(MessageConst.PROPERTY_MESSAGE_TYPE);
        boolean isReply = msgType != null && msgType.equals(MixAll.REPLY_MESSAGE_FLAG);
        if (isReply) {
            if (sendSmartMsg) {
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE_V2, requestHeaderV2);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_REPLY_MESSAGE, requestHeader);
            }
        } else {
            if (sendSmartMsg || msg instanceof MessageBatch) {
                SendMessageRequestHeaderV2 requestHeaderV2 = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV2(requestHeader);
                request = RemotingCommand.createRequestCommand(msg instanceof MessageBatch ? RequestCode.SEND_BATCH_MESSAGE : RequestCode.SEND_MESSAGE_V2, requestHeaderV2);
            } else {
                request = RemotingCommand.createRequestCommand(RequestCode.SEND_MESSAGE, requestHeader);
            }
        }
        //endregion

        request.setBody(msg.getBody());

        switch (communicationMode) {
            case ONEWAY:
                this.remotingClient.invokeOneway(addr, request, timeoutMillis);
                return null;
            case ASYNC:
                final AtomicInteger times = new AtomicInteger();

                //region 发送前超时检测
                long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeAsync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                //endregion

                this.sendMessageAsync(addr, brokerName, msg, timeoutMillis - costTimeAsync, request, sendCallback, topicPublishInfo, instance,
                        retryTimesWhenSendFailed, times, context, producer);
                return null;
            case SYNC:
                //region 发送前超时检测
                long costTimeSync = System.currentTimeMillis() - beginStartTime;
                if (timeoutMillis < costTimeSync) {
                    throw new RemotingTooMuchRequestException("sendMessage call timeout");
                }
                //endregion

                return this.sendMessageSync(addr, brokerName, msg, timeoutMillis - costTimeSync, request);
            default:
                assert false;
                break;
        }

        return null;
    }

    private SendResult sendMessageSync(String addr, String brokerName, Message msg, long timeoutMillis, RemotingCommand request)
            throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        //解析响应结果返回
        return this.processSendResponse(brokerName, msg, response, addr);
    }

    private void sendMessageAsync(String addr, String brokerName, Message msg, long timeoutMillis, RemotingCommand request, SendCallback sendCallback,
                                  TopicPublishInfo topicPublishInfo, MQClientInstance instance, int retryTimesWhenSendFailed, AtomicInteger times,
                                  SendMessageContext context, DefaultMQProducerImpl producer) throws InterruptedException, RemotingException {
        long beginStartTime = System.currentTimeMillis();
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, responseFuture -> {
            long cost = System.currentTimeMillis() - beginStartTime;
            RemotingCommand response = responseFuture.getResponseCommand();
            if (null == sendCallback && response != null) {

                try {
                    SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response, addr);
                    if (context != null) {
                        context.setSendResult(sendResult);
                        context.getProducer().executeSendMessageHookAfter(context);
                    }
                } catch (Throwable ignored) {
                }

                producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), false);
                return;
            }

            if (response != null) {
                try {
                    SendResult sendResult = MQClientAPIImpl.this.processSendResponse(brokerName, msg, response, addr);
                    if (context != null) {
                        context.setSendResult(sendResult);
                        context.getProducer().executeSendMessageHookAfter(context);
                    }

                    //执行客户端回调
                    try {
                        sendCallback.onSuccess(sendResult);
                    } catch (Throwable ignored) {
                    }

                    producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), false);
                } catch (Exception e) {
                    producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), true);
                    onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                            retryTimesWhenSendFailed, times, e, context, false, producer);
                }
            } else {
                producer.updateFaultItem(brokerName, System.currentTimeMillis() - responseFuture.getBeginTimestamp(), true);
                if (!responseFuture.isSendRequestOK()) {
                    MQClientException ex = new MQClientException("send request failed", responseFuture.getCause());
                    onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                            retryTimesWhenSendFailed, times, ex, context, true, producer);
                } else if (responseFuture.isTimeout()) {
                    MQClientException ex = new MQClientException("wait response timeout " + responseFuture.getTimeoutMillis() + "ms",
                            responseFuture.getCause());
                    onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                            retryTimesWhenSendFailed, times, ex, context, true, producer);
                } else {
                    MQClientException ex = new MQClientException("unknow reseaon", responseFuture.getCause());
                    onExceptionImpl(brokerName, msg, timeoutMillis - cost, request, sendCallback, topicPublishInfo, instance,
                            retryTimesWhenSendFailed, times, ex, context, true, producer);
                }
            }
        });
    }

    private void onExceptionImpl(String brokerName, Message msg, long timeoutMillis, RemotingCommand request, SendCallback sendCallback,
                                 TopicPublishInfo topicPublishInfo, MQClientInstance instance, int timesTotal, AtomicInteger curTimes, Exception e,
                                 SendMessageContext context, boolean needRetry, DefaultMQProducerImpl producer) {
        int tmp = curTimes.incrementAndGet();
        if (needRetry && tmp <= timesTotal) {
            String retryBrokerName = brokerName;//by default, it will send to the same broker
            if (topicPublishInfo != null) { //select one message queue accordingly, in order to determine which broker to send
                MessageQueue mqChosen = producer.selectOneMessageQueue(topicPublishInfo, brokerName);
                retryBrokerName = mqChosen.getBrokerName();
            }
            String addr = instance.findBrokerAddressInPublish(retryBrokerName);
            log.info("async send msg by retry {} times. topic={}, brokerAddr={}, brokerName={}", tmp, msg.getTopic(), addr,
                    retryBrokerName);
            try {
                request.setOpaque(RemotingCommand.createNewRequestId());
                sendMessageAsync(addr, retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance,
                        timesTotal, curTimes, context, producer);
            } catch (InterruptedException e1) {
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, false, producer);
            } catch (RemotingConnectException e1) {
                producer.updateFaultItem(brokerName, 3000, true);
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, true, producer);
            } catch (RemotingTooMuchRequestException e1) {
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, false, producer);
            } catch (RemotingException e1) {
                producer.updateFaultItem(brokerName, 3000, true);
                onExceptionImpl(retryBrokerName, msg, timeoutMillis, request, sendCallback, topicPublishInfo, instance, timesTotal, curTimes, e1,
                        context, true, producer);
            }
        } else {
            if (context != null) {
                context.setException(e);
                context.getProducer().executeSendMessageHookAfter(context);
            }

            try {
                sendCallback.onException(e);
            } catch (Exception ignored) {
            }
        }
    }

    private SendResult processSendResponse(String brokerName, Message msg, RemotingCommand response, String addr) throws MQBrokerException, RemotingCommandException {
        SendStatus sendStatus;
        //region 适配服务端响应码与客户端发送状态码
        switch (response.getCode()) {
            case ResponseCode.FLUSH_DISK_TIMEOUT: {
                sendStatus = SendStatus.FLUSH_DISK_TIMEOUT;
                break;
            }
            case ResponseCode.FLUSH_SLAVE_TIMEOUT: {
                sendStatus = SendStatus.FLUSH_SLAVE_TIMEOUT;
                break;
            }
            case ResponseCode.SLAVE_NOT_AVAILABLE: {
                sendStatus = SendStatus.SLAVE_NOT_AVAILABLE;
                break;
            }
            case ResponseCode.SUCCESS: {
                sendStatus = SendStatus.SEND_OK;
                break;
            }
            default: {
                throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
            }
        }
        //endregion

        SendMessageResponseHeader responseHeader = (SendMessageResponseHeader) response.decodeCommandCustomHeader(SendMessageResponseHeader.class);

        //If namespace not null , reset Topic without namespace.
        String topic = msg.getTopic();
        if (StringUtils.isNotEmpty(this.clientConfig.getNamespace())) {
            topic = NamespaceUtil.withoutNamespace(topic, this.clientConfig.getNamespace());
        }

        MessageQueue messageQueue = new MessageQueue(topic, brokerName, responseHeader.getQueueId());

        String uniqMsgId = MessageClientIDSetter.getUniqID(msg);
        if (msg instanceof MessageBatch) {
            StringBuilder sb = new StringBuilder();
            for (Message message : (MessageBatch) msg) {
                sb.append(sb.length() == 0 ? "" : ",").append(MessageClientIDSetter.getUniqID(message));
            }
            uniqMsgId = sb.toString();
        }
        SendResult sendResult = new SendResult(sendStatus, uniqMsgId, responseHeader.getMsgId(), messageQueue, responseHeader.getQueueOffset());
        sendResult.setTransactionId(responseHeader.getTransactionId());
        String regionId = response.getExtFields().get(MessageConst.PROPERTY_MSG_REGION);
        String traceOn = response.getExtFields().get(MessageConst.PROPERTY_TRACE_SWITCH);
        if (regionId == null || regionId.isEmpty()) {
            regionId = MixAll.DEFAULT_TRACE_REGION_ID;
        }
        if (traceOn != null && traceOn.equals("false")) {
            sendResult.setTraceOn(false);
        } else {
            sendResult.setTraceOn(true);
        }
        sendResult.setRegionId(regionId);
        return sendResult;
    }
    //endregion

    //region pull消息
    public PullResult pullMessage(String addr, PullMessageRequestHeader requestHeader, long timeoutMillis, CommunicationMode communicationMode, PullCallback pullCallback)
            throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PULL_MESSAGE, requestHeader);

        switch (communicationMode) {
            case ONEWAY:
                assert false;
                return null;
            case ASYNC:
                this.pullMessageAsync(addr, request, timeoutMillis, pullCallback);
                return null;
            case SYNC:
                return this.pullMessageSync(addr, request, timeoutMillis);
            default:
                assert false;
                break;
        }

        return null;
    }

    private void pullMessageAsync(String addr, RemotingCommand request, long timeoutMillis, PullCallback pullCallback) throws RemotingException, InterruptedException {
        this.remotingClient.invokeAsync(addr, request, timeoutMillis, responseFuture -> {
            RemotingCommand response = responseFuture.getResponseCommand();
            if (response != null) {
                try {
                    PullResult pullResult = MQClientAPIImpl.this.processPullResponse(response, addr);
                    pullCallback.onSuccess(pullResult);
                } catch (Exception e) {
                    pullCallback.onException(e);
                }
            } else {
                if (!responseFuture.isSendRequestOK()) {
                    pullCallback.onException(new MQClientException("send request failed to " + addr + ". Request: " + request, responseFuture.getCause()));
                } else if (responseFuture.isTimeout()) {
                    pullCallback.onException(new MQClientException("wait response from " + addr + " timeout :" + responseFuture.getTimeoutMillis() + "ms" + ". Request: " + request,
                            responseFuture.getCause()));
                } else {
                    pullCallback.onException(new MQClientException("unknown reason. addr: " + addr + ", timeoutMillis: " + timeoutMillis + ". Request: " + request, responseFuture.getCause()));
                }
            }
        });
    }

    private PullResult pullMessageSync(String addr, RemotingCommand request, long timeoutMillis) throws RemotingException, InterruptedException, MQBrokerException {
        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        return this.processPullResponse(response, addr);
    }

    private PullResult processPullResponse(RemotingCommand response, String addr) throws MQBrokerException, RemotingCommandException {
        PullStatus pullStatus = PullStatus.NO_NEW_MSG;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS:
                pullStatus = PullStatus.FOUND;
                break;
            case ResponseCode.PULL_NOT_FOUND:
                pullStatus = PullStatus.NO_NEW_MSG;
                break;
            case ResponseCode.PULL_RETRY_IMMEDIATELY:
                pullStatus = PullStatus.NO_MATCHED_MSG;
                break;
            case ResponseCode.PULL_OFFSET_MOVED:
                pullStatus = PullStatus.OFFSET_ILLEGAL;
                break;

            default:
                throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }

        PullMessageResponseHeader responseHeader = (PullMessageResponseHeader) response.decodeCommandCustomHeader(PullMessageResponseHeader.class);

        return new PullResultExt(pullStatus, responseHeader.getNextBeginOffset(), responseHeader.getMinOffset(),
                responseHeader.getMaxOffset(), null, responseHeader.getSuggestWhichBrokerId(), response.getBody());
    }
    //endregion

    /**
     *
     */
    public Set<MessageQueue> lockBatchMQ(String addr, LockBatchRequestBody requestBody, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.LOCK_BATCH_MQ, null);

        request.setBody(requestBody.encode());
        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        if (response.getCode() == ResponseCode.SUCCESS) {
            LockBatchResponseBody responseBody = LockBatchResponseBody.decode(response.getBody(), LockBatchResponseBody.class);
            return responseBody.getLockOKMQSet();
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     *
     */
    public void unlockBatchMQ(String addr, UnlockBatchRequestBody requestBody, long timeoutMillis, boolean oneway)
            throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNLOCK_BATCH_MQ, null);
        request.setBody(requestBody.encode());

        if (oneway) {
            this.remotingClient.invokeOneway(addr, request, timeoutMillis);
        } else {
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            if (response.getCode() == ResponseCode.SUCCESS) {
                return;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }
    }

    /**
     * 创建订阅组
     */
    public void createSubscriptionGroup(String addr, SubscriptionGroupConfig config, long timeoutMillis)
            throws RemotingException, InterruptedException, MQClientException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP, null);

        byte[] body = RemotingSerializable.encode(config);
        request.setBody(body);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQClientException(response.getCode(), response.getRemark());

    }

    /**
     * 创建topic
     * 更新topicmanager topic配置
     * 向其他所有 nameserver 注册更新相关路由信息
     */
    public void createTopic(String addr, String defaultTopic, TopicConfig topicConfig, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        CreateTopicRequestHeader requestHeader = new CreateTopicRequestHeader();
        requestHeader.setTopic(topicConfig.getTopicName());
        requestHeader.setDefaultTopic(defaultTopic);
        requestHeader.setReadQueueNums(topicConfig.getReadQueueNums());
        requestHeader.setWriteQueueNums(topicConfig.getWriteQueueNums());
        requestHeader.setPerm(topicConfig.getPerm());
        requestHeader.setTopicFilterType(topicConfig.getTopicFilterType().name());
        requestHeader.setTopicSysFlag(topicConfig.getTopicSysFlag());
        requestHeader.setOrder(topicConfig.isOrder());

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 创建账户 accesstoken acl配置
     */
    public void createPlainAccessConfig(String addr, PlainAccessConfig plainAccessConfig, long timeoutMillis)
            throws RemotingException, InterruptedException, MQClientException {
        CreateAccessConfigRequestHeader requestHeader = new CreateAccessConfigRequestHeader();
        requestHeader.setAccessKey(plainAccessConfig.getAccessKey());
        requestHeader.setSecretKey(plainAccessConfig.getSecretKey());
        requestHeader.setAdmin(plainAccessConfig.isAdmin());
        requestHeader.setDefaultGroupPerm(plainAccessConfig.getDefaultGroupPerm());
        requestHeader.setDefaultTopicPerm(plainAccessConfig.getDefaultTopicPerm());
        requestHeader.setWhiteRemoteAddress(plainAccessConfig.getWhiteRemoteAddress());
        requestHeader.setTopicPerms(UtilAll.list2String(plainAccessConfig.getTopicPerms(), ","));
        requestHeader.setGroupPerms(UtilAll.list2String(plainAccessConfig.getGroupPerms(), ","));

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_AND_CREATE_ACL_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);


        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 删除账户 accesstoken acl配置
     */
    public void deleteAccessConfig(String addr, String accessKey, long timeoutMillis)
            throws RemotingException, InterruptedException, MQClientException {
        DeleteAccessConfigRequestHeader requestHeader = new DeleteAccessConfigRequestHeader();
        requestHeader.setAccessKey(accessKey);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_ACL_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 更新全局白名单
     */
    public void updateGlobalWhiteAddrsConfig(final String addr, final String globalWhiteAddrs, final long timeoutMillis)
            throws RemotingException, InterruptedException, MQClientException {

        UpdateGlobalWhiteAddrsConfigRequestHeader requestHeader = new UpdateGlobalWhiteAddrsConfigRequestHeader();
        requestHeader.setGlobalWhiteAddrs(globalWhiteAddrs);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 获取broker acl配置版本号
     */
    public ClusterAclVersionInfo getBrokerClusterAclInfo(String addr, long timeoutMillis)
            throws RemotingCommandException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            GetBrokerAclConfigResponseHeader responseHeader = (GetBrokerAclConfigResponseHeader) response.decodeCommandCustomHeader(GetBrokerAclConfigResponseHeader.class);
            ClusterAclVersionInfo clusterAclVersionInfo = new ClusterAclVersionInfo();
            clusterAclVersionInfo.setClusterName(responseHeader.getClusterName());
            clusterAclVersionInfo.setBrokerName(responseHeader.getBrokerName());
            clusterAclVersionInfo.setBrokerAddr(responseHeader.getBrokerAddr());
            clusterAclVersionInfo.setAclConfigDataVersion(DataVersion.fromJson(responseHeader.getVersion(), DataVersion.class));
            return clusterAclVersionInfo;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);

    }

    /**
     * 获取broker所有 acl配置和全局白名单
     */
    public AclConfig getBrokerClusterConfig(final String addr, final long timeoutMillis) throws InterruptedException, RemotingTimeoutException,
            RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_ACL_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            if (response.getBody() != null) {
                GetBrokerClusterAclConfigResponseBody body =
                        GetBrokerClusterAclConfigResponseBody.decode(response.getBody(), GetBrokerClusterAclConfigResponseBody.class);
                AclConfig aclConfig = new AclConfig();
                aclConfig.setGlobalWhiteAddrs(body.getGlobalWhiteAddrs());
                aclConfig.setPlainAccessConfigs(body.getPlainAccessConfigs());
                return aclConfig;
            }
        }
        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);

    }

    /**
     * 根据offset查找特定消息
     */
    public MessageExt viewMessage(String addr, long phyoffset, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        ViewMessageRequestHeader requestHeader = new ViewMessageRequestHeader();
        requestHeader.setOffset(phyoffset);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_MESSAGE_BY_ID, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(response.getBody());
            MessageExt messageExt = MessageDecoder.clientDecode(byteBuffer, true);
            //If namespace not null , reset Topic without namespace.
            if (StringUtils.isNotEmpty(this.clientConfig.getNamespace())) {
                messageExt.setTopic(NamespaceUtil.withoutNamespace(messageExt.getTopic(), this.clientConfig.getNamespace()));
            }
            return messageExt;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 查找特定时间存储的消息offset
     */
    public long searchOffset(String addr, String topic, int queueId, long timestamp, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        SearchOffsetRequestHeader requestHeader = new SearchOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setTimestamp(timestamp);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.SEARCH_OFFSET_BY_TIMESTAMP, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.decodeCommandCustomHeader(SearchOffsetResponseHeader.class);
            return responseHeader.getOffset();
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 查询queue最大offset
     */
    public long getMaxOffset(String addr, String topic, int queueId, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        GetMaxOffsetRequestHeader requestHeader = new GetMaxOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MAX_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.decodeCommandCustomHeader(GetMaxOffsetResponseHeader.class);
            return responseHeader.getOffset();
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 获取消费组下所有在线消费者clientID
     */
    public List<String> getConsumerIdListByGroup(String addr, String consumerGroup, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        GetConsumerListByGroupRequestHeader requestHeader = new GetConsumerListByGroupRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_LIST_BY_GROUP, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            if (response.getBody() != null) {
                GetConsumerListByGroupResponseBody body = GetConsumerListByGroupResponseBody.decode(response.getBody(), GetConsumerListByGroupResponseBody.class);
                return body.getConsumerIdList();
            }
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 查询queue最小offset
     */
    public long getMinOffset(String addr, String topic, int queueId, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        GetMinOffsetRequestHeader requestHeader = new GetMinOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_MIN_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            GetMinOffsetResponseHeader responseHeader =
                    (GetMinOffsetResponseHeader) response.decodeCommandCustomHeader(GetMinOffsetResponseHeader.class);

            return responseHeader.getOffset();
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 查找queue最早消息的存储时间
     */
    public long getEarliestMsgStoretime(String addr, String topic, int queueId, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        GetEarliestMsgStoretimeRequestHeader requestHeader = new GetEarliestMsgStoretimeRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_EARLIEST_MSG_STORETIME, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response.decodeCommandCustomHeader(GetEarliestMsgStoretimeResponseHeader.class);
            return responseHeader.getTimestamp();
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 查询消费组对指定queue的消费offset
     */
    public long queryConsumerOffset(String addr, QueryConsumerOffsetRequestHeader requestHeader, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUMER_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            QueryConsumerOffsetResponseHeader responseHeader = (QueryConsumerOffsetResponseHeader) response.decodeCommandCustomHeader(QueryConsumerOffsetResponseHeader.class);

            return responseHeader.getOffset();
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 向broker提交消费进度
     */
    public void updateConsumerOffset(String addr, UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis)
            throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * oneway方式向broker提交消费进度
     */
    public void updateConsumerOffsetOneway(String addr, UpdateConsumerOffsetRequestHeader requestHeader, long timeoutMillis)
            throws RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_CONSUMER_OFFSET, requestHeader);

        this.remotingClient.invokeOneway(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
    }

    /**
     * 发送心跳 更新consumer、producer、subscription等信息
     */
    public int sendHearbeat(String addr, HeartbeatData heartbeatData, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.HEART_BEAT, null);
        request.setLanguage(clientConfig.getLanguage());
        request.setBody(heartbeatData.encode());

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return response.getVersion();
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 下线生产者或消费者
     * 移除管理的生产者
     * 移除管理的consumer 通知consumer变更重平衡等
     */
    public void unregisterClient(String addr, String clientID, String producerGroup, String consumerGroup, long timeoutMillis) throws RemotingException, MQBrokerException, InterruptedException {
        final UnregisterClientRequestHeader requestHeader = new UnregisterClientRequestHeader();
        requestHeader.setClientID(clientID);
        requestHeader.setProducerGroup(producerGroup);
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UNREGISTER_CLIENT, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 本地事务执行完后 根据结果向broker提交或回滚事务
     */
    public void endTransactionOneway(String addr, EndTransactionRequestHeader requestHeader, String remark, long timeoutMillis)
            throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.END_TRANSACTION, requestHeader);

        request.setRemark(remark);
        this.remotingClient.invokeOneway(addr, request, timeoutMillis);
    }

    /**
     * 查询时间范围内指定数量消息
     */
    public void queryMessage(String addr, QueryMessageRequestHeader requestHeader, long timeoutMillis, InvokeCallback invokeCallback, Boolean isUnqiueKey) throws RemotingException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_MESSAGE, requestHeader);
        request.addExtField(MixAll.UNIQUE_MSG_QUERY_FLAG, isUnqiueKey.toString());
        this.remotingClient.invokeAsync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis, invokeCallback);
    }

    /**
     * 消费失败返回消息到broker重试queue中
     */
    public void consumerSendMessageBack(String addr, MessageExt msg, String consumerGroup, int delayLevel, long timeoutMillis, int maxConsumeRetryTimes)
            throws RemotingException, MQBrokerException, InterruptedException {
        ConsumerSendMsgBackRequestHeader requestHeader = new ConsumerSendMsgBackRequestHeader();
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUMER_SEND_MSG_BACK, requestHeader);

        requestHeader.setGroup(consumerGroup);
        requestHeader.setOriginTopic(msg.getTopic());
        requestHeader.setOffset(msg.getCommitLogOffset());
        requestHeader.setDelayLevel(delayLevel);
        requestHeader.setOriginMsgId(msg.getMsgId());
        requestHeader.setMaxReconsumeTimes(maxConsumeRetryTimes);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 获取topic下各个queue最大最小offset。最后存储时间
     */
    public TopicStatsTable getTopicStatsInfo(String addr, String topic, long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        GetTopicStatsInfoRequestHeader requestHeader = new GetTopicStatsInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPIC_STATS_INFO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        if (response.getCode() == ResponseCode.SUCCESS) {
            return TopicStatsTable.decode(response.getBody(), TopicStatsTable.class);
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 获取消费组进度统计
     */
    public ConsumeStats getConsumeStats(String addr, String consumerGroup, long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        return getConsumeStats(addr, consumerGroup, null, timeoutMillis);
    }

    /**
     * 获取消费组进度统计
     */
    public ConsumeStats getConsumeStats(String addr, String consumerGroup, String topic, long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        GetConsumeStatsRequestHeader requestHeader = new GetConsumeStatsRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUME_STATS, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        if (response.getCode() == ResponseCode.SUCCESS) {
            return ConsumeStats.decode(response.getBody(), ConsumeStats.class);
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 获取生产组下所有生产者连接
     */
    public ProducerConnection getProducerConnectionList(String addr, String producerGroup, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        GetProducerConnectionListRequestHeader requestHeader = new GetProducerConnectionListRequestHeader();
        requestHeader.setProducerGroup(producerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_PRODUCER_CONNECTION_LIST, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        if (response.getCode() == ResponseCode.SUCCESS) {
            return ProducerConnection.decode(response.getBody(), ProducerConnection.class);
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 获取消费组所有订阅配置和所有消费者连接
     */
    public ConsumerConnection getConsumerConnectionList(String addr, String consumerGroup, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        GetConsumerConnectionListRequestHeader requestHeader = new GetConsumerConnectionListRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_CONNECTION_LIST, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
        if (response.getCode() == ResponseCode.SUCCESS) {
            return ConsumerConnection.decode(response.getBody(), ConsumerConnection.class);
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 获取broker 所有运行时信息
     * 各类统计数据: 拉取发送消息数、各请求队列大小、延时等
     */
    public KVTable getBrokerRuntimeInfo(String addr, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_RUNTIME_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        if (response.getCode() == ResponseCode.SUCCESS) {
            return KVTable.decode(response.getBody(), KVTable.class);
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 更新broker 配置
     * 若存在brokerpermission配置则重新向所有nameserver注册更新路由信息
     */
    public void updateBrokerConfig(String addr, Properties properties, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, UnsupportedEncodingException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);

        String str = MixAll.properties2String(properties);
        if (str.length() > 0) {
            request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);
            if (response.getCode() == ResponseCode.SUCCESS) {
                return;
            }
            throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
        }
    }

    /**
     * 获取broker所有配置
     * broker配置 nettyserver&client配置 messagestore配置等
     */
    public Properties getBrokerConfig(String addr, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException, UnsupportedEncodingException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET));
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 获取cluster信息
     * 所有brokername
     * brokername下所有brokeraddr
     */
    public ClusterInfo getBrokerClusterInfo(long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return ClusterInfo.decode(response.getBody(), ClusterInfo.class);
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }

    /**
     * 向nameserver 查询topic路由信息
     */
    public TopicRouteData getDefaultTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {

        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, false);
    }

    /**
     * 向nameserver 查询topic路由信息
     */
    public TopicRouteData getTopicRouteInfoFromNameServer(final String topic, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {

        return getTopicRouteInfoFromNameServer(topic, timeoutMillis, true);
    }

    /**
     * 向nameserver 查询topic路由信息
     */
    public TopicRouteData getTopicRouteInfoFromNameServer(String topic, long timeoutMillis, boolean allowTopicNotExist)
            throws MQClientException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException {
        GetRouteInfoRequestHeader requestHeader = new GetRouteInfoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ROUTEINFO_BY_TOPIC, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.TOPIC_NOT_EXIST: {
                if (allowTopicNotExist) {
                    log.warn("get Topic [{}] RouteInfoFromNameServer is not exist value", topic);
                }
                break;
            }
            case ResponseCode.SUCCESS: {
                byte[] body = response.getBody();
                if (body != null) {
                    return TopicRouteData.decode(body, TopicRouteData.class);
                }
            }
            default:
                break;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 从nameserver拉去所有topic名
     */
    public TopicList getTopicListFromNameServer(long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            byte[] body = response.getBody();
            if (body != null) {
                return TopicList.decode(body, TopicList.class);
            }
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 移除路由信息中该brokername下所有queue的写权限
     * 影响topic数量
     */
    public int wipeWritePermOfBroker(String namesrvAddr, String brokerName, long timeoutMillis)
            throws RemotingCommandException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQClientException {
        WipeWritePermOfBrokerRequestHeader requestHeader = new WipeWritePermOfBrokerRequestHeader();
        requestHeader.setBrokerName(brokerName);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.WIPE_WRITE_PERM_OF_BROKER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            WipeWritePermOfBrokerResponseHeader responseHeader = (WipeWritePermOfBrokerResponseHeader) response.decodeCommandCustomHeader(WipeWritePermOfBrokerResponseHeader.class);
            return responseHeader.getWipeTopicCount();
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 删除该topic config并清理数据文件
     */
    public void deleteTopicInBroker(String addr, String topic, long timeoutMillis) throws RemotingException, InterruptedException, MQClientException {
        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_BROKER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 删除该topic的路由信息
     */
    public void deleteTopicInNameServer(String addr, String topic, long timeoutMillis) throws RemotingException, InterruptedException, MQClientException {
        DeleteTopicRequestHeader requestHeader = new DeleteTopicRequestHeader();
        requestHeader.setTopic(topic);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_TOPIC_IN_NAMESRV, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(addr, request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 删除该订阅组
     */
    public void deleteSubscriptionGroup(String addr, String groupName, long timeoutMillis) throws RemotingException, InterruptedException, MQClientException {
        DeleteSubscriptionGroupRequestHeader requestHeader = new DeleteSubscriptionGroupRequestHeader();
        requestHeader.setGroupName(groupName);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_SUBSCRIPTIONGROUP, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 在该namespace下查询该key配置
     */
    public String getKVConfigValue(String namespace, String key, long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        GetKVConfigRequestHeader requestHeader = new GetKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KV_CONFIG, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            GetKVConfigResponseHeader responseHeader = (GetKVConfigResponseHeader) response.decodeCommandCustomHeader(GetKVConfigResponseHeader.class);
            return responseHeader.getValue();
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 向该namespace下新增kv配置
     */
    public void putKVConfigValue(final String namespace, final String key, final String value, final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        PutKVConfigRequestHeader requestHeader = new PutKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);
        requestHeader.setValue(value);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.PUT_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                if (response.getCode() != ResponseCode.SUCCESS) {
                    errResponse = response;
                }
            }

            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    /**
     * 删除所有nameserver中kvtable中key值
     */
    public void deleteKVConfigValue(final String namespace, final String key, final long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        DeleteKVConfigRequestHeader requestHeader = new DeleteKVConfigRequestHeader();
        requestHeader.setNamespace(namespace);
        requestHeader.setKey(key);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.DELETE_KV_CONFIG, requestHeader);

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            RemotingCommand errResponse = null;
            for (String namesrvAddr : nameServerAddressList) {
                RemotingCommand response = this.remotingClient.invokeSync(namesrvAddr, request, timeoutMillis);
                assert response != null;
                if (response.getCode() != ResponseCode.SUCCESS) {
                    errResponse = response;
                }
            }
            if (errResponse != null) {
                throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
            }
        }
    }

    /**
     * 获取默认nameserver指定namespace下kvtable
     */
    public KVTable getKVListByNamespace(final String namespace, final long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetKVListByNamespaceRequestHeader requestHeader = new GetKVListByNamespaceRequestHeader();
        requestHeader.setNamespace(namespace);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_KVLIST_BY_NAMESPACE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);
        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return KVTable.decode(response.getBody(), KVTable.class);
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 让组内所有consumers所有queue重置消费进度到指定时间的消息offset
     */
    public Map<MessageQueue, Long> invokeBrokerToResetOffset(String addr, String topic, String group, long timestamp, boolean isForce, long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        return invokeBrokerToResetOffset(addr, topic, group, timestamp, isForce, timeoutMillis, false);
    }

    /**
     * 让组内所有consumers所有queue重置消费进度到指定时间的消息offset
     */
    public Map<MessageQueue, Long> invokeBrokerToResetOffset(String addr, String topic, String group, long timestamp, boolean isForce, long timeoutMillis, boolean isC)
            throws RemotingException, MQClientException, InterruptedException {
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timestamp);
        requestHeader.setForce(isForce);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_RESET_OFFSET, requestHeader);
        if (isC) {
            request.setLanguage(LanguageCode.CPP);
        }

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            if (response.getBody() != null) {
                ResetOffsetBody body = ResetOffsetBody.decode(response.getBody(), ResetOffsetBody.class);
                return body.getOffsetTable();
            }
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 获取各consumer对各queue的消费offset
     */
    public Map<String/*clientId*/, Map<MessageQueue, Long>> invokeBrokerToGetConsumerStatus(String addr, final String topic, String group, String clientAddr, long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setClientAddr(clientAddr);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            if (response.getBody() != null) {
                GetConsumerStatusBody body = GetConsumerStatusBody.decode(response.getBody(), GetConsumerStatusBody.class);
                return body.getConsumerTable();
            }
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 查询topic有哪些consumergroup消费
     */
    public GroupList queryTopicConsumeByWho(String addr, String topic, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        QueryTopicConsumeByWhoRequestHeader requestHeader = new QueryTopicConsumeByWhoRequestHeader();
        requestHeader.setTopic(topic);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_TOPIC_CONSUME_BY_WHO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        if (response.getCode() == ResponseCode.SUCCESS) {
            return GroupList.decode(response.getBody(), GroupList.class);
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 查询各queue内消息最大最小存储时间、group消费进度、两者时间差值等
     */
    public List<QueueTimeSpan> queryConsumeTimeSpan(String addr, String topic, String group, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        QueryConsumeTimeSpanRequestHeader requestHeader = new QueryConsumeTimeSpanRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_TIME_SPAN, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        if (response.getCode() == ResponseCode.SUCCESS) {
            QueryConsumeTimeSpanBody consumeTimeSpanBody = GroupList.decode(response.getBody(), QueryConsumeTimeSpanBody.class);
            return consumeTimeSpanBody.getConsumeTimeSpanSet();
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 查询nameserver cluster下有哪些topic
     */
    public TopicList getTopicsByCluster(String cluster, long timeoutMillis) throws RemotingException, MQClientException, InterruptedException {
        GetTopicsByClusterRequestHeader requestHeader = new GetTopicsByClusterRequestHeader();
        requestHeader.setCluster(cluster);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_TOPICS_BY_CLUSTER, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            byte[] body = response.getBody();
            if (body != null) {
                return TopicList.decode(body, TopicList.class);
            }
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }


    /**
     * 清理 最大offset在commitlog已经被删除了的consume queue
     */
    public boolean cleanExpiredConsumeQueue(String addr, long timeoutMillis)
            throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        if (response.getCode() == ResponseCode.SUCCESS) {
            return true;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 清理未使用的topic
     * 删除commilog、consumequeue、index等
     */
    public boolean cleanUnusedTopicByAddr(String addr, long timeoutMillis)
            throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLEAN_UNUSED_TOPIC, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        if (response.getCode() == ResponseCode.SUCCESS) {
            return true;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 拉去指定consumer运行信息
     * 消费者配置
     * 运行统计 pulltps、pullrt等
     * 订阅配置
     * 拉取队列状态
     */
    public ConsumerRunningInfo getConsumerRunningInfo(String addr, String consumerGroup, String clientId, boolean jstack, long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        GetConsumerRunningInfoRequestHeader requestHeader = new GetConsumerRunningInfoRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setJstackEnable(jstack);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_RUNNING_INFO, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            byte[] body = response.getBody();
            if (body != null) {
                return ConsumerRunningInfo.decode(body, ConsumerRunningInfo.class);
            }
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 指定消费者直接消费消息
     * 发送至broker 查出消息后转发至 clientId对应consumer
     */
    public ConsumeMessageDirectlyResult consumeMessageDirectly(String addr, String consumerGroup, String clientId, String msgId, long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        ConsumeMessageDirectlyResultRequestHeader requestHeader = new ConsumeMessageDirectlyResultRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        requestHeader.setClientId(clientId);
        requestHeader.setMsgId(msgId);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            byte[] body = response.getBody();
            if (body != null) {
                return ConsumeMessageDirectlyResult.decode(body, ConsumeMessageDirectlyResult.class);
            }
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }


    /**
     * 将源group消费进度复制给目标group
     */
    public void cloneGroupOffset(String addr, final String srcGroup, final String destGroup, final String topic, boolean isOffline, long timeoutMillis)
            throws RemotingException, MQClientException, InterruptedException {
        CloneGroupOffsetRequestHeader requestHeader = new CloneGroupOffsetRequestHeader();
        requestHeader.setSrcGroup(srcGroup);
        requestHeader.setDestGroup(destGroup);
        requestHeader.setTopic(topic);
        requestHeader.setOffline(isOffline);
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CLONE_GROUP_OFFSET, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return;
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 获取broker统计信息
     */
    public BrokerStatsData viewBrokerStatsData(String brokerAddr, String statsName, String statsKey, long timeoutMillis)
            throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        ViewBrokerStatsDataRequestHeader requestHeader = new ViewBrokerStatsDataRequestHeader();
        requestHeader.setStatsName(statsName);
        requestHeader.setStatsKey(statsKey);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.VIEW_BROKER_STATS_DATA, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);
        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            byte[] body = response.getBody();
            if (body != null) {
                return BrokerStatsData.decode(body, BrokerStatsData.class);
            }
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 获取集群名 默认空
     */
    public Set<String> getClusterList(String topic, long timeoutMillis)
            throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        return new HashSet<>();
    }

    /**
     * 查询订阅组对各个topic各个queue的消费状态
     * brokeroffset 最大offset
     * consumeroffset 消费offset
     * 进度差值、最后存储时间、tps等
     */
    public ConsumeStatsList fetchConsumeStatsInBroker(String brokerAddr, boolean isOrder, long timeoutMillis)
            throws MQClientException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        GetConsumeStatsInBrokerHeader requestHeader = new GetConsumeStatsInBrokerHeader();
        requestHeader.setIsOrder(isOrder);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CONSUME_STATS, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            byte[] body = response.getBody();
            if (body != null) {
                return ConsumeStatsList.decode(body, ConsumeStatsList.class);
            }
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 查询所有订阅组
     */
    public SubscriptionGroupWrapper getAllSubscriptionGroup(String brokerAddr, long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return SubscriptionGroupWrapper.decode(response.getBody(), SubscriptionGroupWrapper.class);
        }
        throw new MQBrokerException(response.getCode(), response.getRemark(), brokerAddr);
    }

    /**
     * 向broker查询所有topic及topic配置
     */
    public TopicConfigSerializeWrapper getAllTopicConfig(String addr, long timeoutMillis)
            throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG, null);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr), request, timeoutMillis);

        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return TopicConfigSerializeWrapper.decode(response.getBody(), TopicConfigSerializeWrapper.class);
        }

        throw new MQBrokerException(response.getCode(), response.getRemark(), addr);
    }

    /**
     * 更新各个nameserver 配置
     */
    public void updateNameServerConfig(Properties properties, List<String> nameServers, long timeoutMillis)
            throws UnsupportedEncodingException, InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {
        String str = MixAll.properties2String(properties);
        if (str.length() < 1) {
            return;
        }

        List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ? this.remotingClient.getNameServerAddressList() : nameServers;
        if (invokeNameServers == null || invokeNameServers.isEmpty()) {
            return;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_NAMESRV_CONFIG, null);
        request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));

        RemotingCommand errResponse = null;
        for (String nameServer : invokeNameServers) {
            RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);
            assert response != null;
            if (response.getCode() != ResponseCode.SUCCESS) {
                errResponse = response;
            }
        }

        if (errResponse != null) {
            throw new MQClientException(errResponse.getCode(), errResponse.getRemark());
        }
    }

    /**
     * 获取各个nameserver 配置
     */
    public Map<String/* nameserver address */, Properties> getNameServerConfig(List<String> nameServers, long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException, UnsupportedEncodingException {
        List<String> invokeNameServers = (nameServers == null || nameServers.isEmpty()) ? this.remotingClient.getNameServerAddressList() : nameServers;
        if (invokeNameServers == null || invokeNameServers.isEmpty()) {
            return null;
        }

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_NAMESRV_CONFIG, null);

        Map<String, Properties> configMap = new HashMap<>(4);
        for (String nameServer : invokeNameServers) {
            RemotingCommand response = this.remotingClient.invokeSync(nameServer, request, timeoutMillis);

            assert response != null;

            if (ResponseCode.SUCCESS == response.getCode()) {
                configMap.put(nameServer, MixAll.string2Properties(new String(response.getBody(), MixAll.DEFAULT_CHARSET)));
            } else {
                throw new MQClientException(response.getCode(), response.getRemark());
            }
        }
        return configMap;
    }

    /**
     * 获取consume queue index后所有数据、最大最小offset、订阅数据
     */
    public QueryConsumeQueueResponseBody queryConsumeQueue(String brokerAddr, String topic, int queueId, long index, int count, String consumerGroup, long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {

        QueryConsumeQueueRequestHeader requestHeader = new QueryConsumeQueueRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setQueueId(queueId);
        requestHeader.setIndex(index);
        requestHeader.setCount(count);
        requestHeader.setConsumerGroup(consumerGroup);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.QUERY_CONSUME_QUEUE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

        assert response != null;
        if (ResponseCode.SUCCESS == response.getCode()) {
            return QueryConsumeQueueResponseBody.decode(response.getBody(), QueryConsumeQueueResponseBody.class);
        }

        throw new MQClientException(response.getCode(), response.getRemark());
    }

    /**
     * 想服务端校验消费组订阅配置
     * 过滤类型为tags pass
     * 过滤类型为表达式 broker需开启 property filter & 表达式解析正确
     */
    public void checkClientInBroker(String brokerAddr, String consumerGroup, String clientId, SubscriptionData subscriptionData, long timeoutMillis)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQClientException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.CHECK_CLIENT_CONFIG, null);

        CheckClientRequestBody requestBody = new CheckClientRequestBody();
        requestBody.setClientId(clientId);
        requestBody.setGroup(consumerGroup);
        requestBody.setSubscriptionData(subscriptionData);
        request.setBody(requestBody.encode());

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), brokerAddr), request, timeoutMillis);

        assert response != null;
        if (ResponseCode.SUCCESS != response.getCode()) {
            throw new MQClientException(response.getCode(), response.getRemark());
        }
    }


    /**
     * 将消息重投至 half topic
     * 并将事务状态检查次数设为 0
     */
    public boolean resumeCheckHalfMessage(String addr, String msgId, long timeoutMillis) throws RemotingException, InterruptedException {
        ResumeCheckHalfMessageRequestHeader requestHeader = new ResumeCheckHalfMessageRequestHeader();
        requestHeader.setMsgId(msgId);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.RESUME_CHECK_HALF_MESSAGE, requestHeader);

        RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(this.clientConfig.isVipChannelEnabled(), addr),
                request, timeoutMillis);
        assert response != null;
        if (response.getCode() == ResponseCode.SUCCESS) {
            return true;
        }
        log.error("Failed to resume half message check logic. Remark={}", response.getRemark());
        return false;
    }
}
