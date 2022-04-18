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

import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.pagecache.OneMessageTransfer;
import org.apache.rocketmq.broker.pagecache.QueryMessageTransfer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.QueryMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryMessageResponseHeader;
import org.apache.rocketmq.common.protocol.header.ViewMessageRequestHeader;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.AsyncNettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.QueryMessageResult;
import org.apache.rocketmq.store.SelectMappedBufferResult;

/**
 * 消息查询命令处理器
 */
public class QueryMessageProcessor extends AsyncNettyRequestProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerController brokerController;

    public QueryMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        switch (request.getCode()) {

            //region key & 存储时间查询 【查询索引 topic#key & store时间范围 获取offsets 然后从commitlog中读取消息】
            case RequestCode.QUERY_MESSAGE:
                return this.queryMessage(ctx, request);
            //endregion

            //region commitlogOffset查询
            case RequestCode.VIEW_MESSAGE_BY_ID:
                return this.viewMessageById(ctx, request);
            //endregion
            default:
                break;
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    public RemotingCommand queryMessage(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {
        //解析命令头 设置响应请求ID
        RemotingCommand response = RemotingCommand.createResponseCommand(QueryMessageResponseHeader.class);
        QueryMessageResponseHeader responseHeader = (QueryMessageResponseHeader) response.readCustomHeader();
        QueryMessageRequestHeader requestHeader = (QueryMessageRequestHeader) request.decodeCommandCustomHeader(QueryMessageRequestHeader.class);
        response.setOpaque(request.getOpaque());

        //设置查询数量
        String isUniqueKey = request.getExtFields().get(MixAll.UNIQUE_MSG_QUERY_FLAG);
        if (isUniqueKey != null && "true".equals(isUniqueKey)) {
            requestHeader.setMaxNum(this.brokerController.getMessageStoreConfig().getDefaultQueryMaxNum());
        }

        //通过messagesotre查询消息
        QueryMessageResult queryMessageResult =
                this.brokerController.getMessageStore().queryMessage(requestHeader.getTopic(),
                        requestHeader.getKey(), requestHeader.getMaxNum(), requestHeader.getBeginTimestamp(),
                        requestHeader.getEndTimestamp());
        assert queryMessageResult != null;

        //设置索引最大offset、最近更新时间
        responseHeader.setIndexLastUpdatePhyoffset(queryMessageResult.getIndexLastUpdatePhyoffset());
        responseHeader.setIndexLastUpdateTimestamp(queryMessageResult.getIndexLastUpdateTimestamp());

        //写回查询结果
        if (queryMessageResult.getBufferTotalSize() > 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            try {
                FileRegion fileRegion = new QueryMessageTransfer(response.encodeHeader(queryMessageResult.getBufferTotalSize()), queryMessageResult);
                ctx.channel().writeAndFlush(fileRegion).addListener((ChannelFutureListener) future -> {
                    queryMessageResult.release();
                    if (!future.isSuccess()) {
                        log.error("transfer query message by page cache failed, ", future.cause());
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                queryMessageResult.release();
            }

            return null;
        }

        response.setCode(ResponseCode.QUERY_NOT_FOUND);
        response.setRemark("can not find message, maybe time range not correct");
        return response;
    }

    public RemotingCommand viewMessageById(ChannelHandlerContext ctx, RemotingCommand request) throws RemotingCommandException {

        //解析命令头 设置响应请求ID
        RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ViewMessageRequestHeader requestHeader = (ViewMessageRequestHeader) request.decodeCommandCustomHeader(ViewMessageRequestHeader.class);
        response.setOpaque(request.getOpaque());

        //通过messagesotre查询offset消息
        SelectMappedBufferResult selectMappedBufferResult = this.brokerController.getMessageStore().selectOneMessageByOffset(requestHeader.getOffset());

        //写回查询结果
        if (selectMappedBufferResult != null) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            try {
                FileRegion fileRegion = new OneMessageTransfer(response.encodeHeader(selectMappedBufferResult.getSize()), selectMappedBufferResult);
                ctx.channel().writeAndFlush(fileRegion).addListener((ChannelFutureListener) future -> {
                    selectMappedBufferResult.release();
                    if (!future.isSuccess()) {
                        log.error("Transfer one message from page cache failed, ", future.cause());
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                selectMappedBufferResult.release();
            }

            return null;
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("can not find message by the offset, " + requestHeader.getOffset());
        }

        return response;
    }
}
