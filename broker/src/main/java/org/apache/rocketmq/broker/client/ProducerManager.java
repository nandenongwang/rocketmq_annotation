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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.var;
import org.apache.rocketmq.broker.util.PositiveAtomicCounter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.common.RemotingUtil;

public class ProducerManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private static final long CHANNEL_EXPIRED_TIMEOUT = 1000 * 120;
    private static final int GET_AVAILABLE_CHANNEL_RETRY_COUNT = 3;
    @Getter
    private final ConcurrentHashMap<String /* producergroup */, ConcurrentHashMap<Channel, ClientChannelInfo>> groupChannelTable = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String/* clientId */, Channel> clientChannelTable = new ConcurrentHashMap<>();

    /**
     * 递增计数器 用于事务检查随机选取producer channel
     */
    private final PositiveAtomicCounter positiveAtomicCounter = new PositiveAtomicCounter();

    /**
     * 移除并关闭默认2分钟未活跃的producer channel
     */
    public void scanNotActiveChannel() {
        for (var entry : this.groupChannelTable.entrySet()) {
            String group = entry.getKey();
            ConcurrentHashMap<Channel, ClientChannelInfo> chlMap = entry.getValue();
            Iterator<Entry<Channel, ClientChannelInfo>> it = chlMap.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Channel, ClientChannelInfo> item = it.next();
                ClientChannelInfo info = item.getValue();
                if (System.currentTimeMillis() - info.getLastUpdateTimestamp() > CHANNEL_EXPIRED_TIMEOUT) {
                    it.remove();
                    clientChannelTable.remove(info.getClientId());
                    log.warn("SCAN: remove expired channel[{}] from ProducerManager groupChannelTable, producer group name: {}",
                            RemotingHelper.parseChannelRemoteAddr(info.getChannel()), group);
                    RemotingUtil.closeChannel(info.getChannel());
                }
            }
        }
    }

    /**
     * 处理producer channel关闭事件
     * 清理内存中该channel相关数据
     */
    public synchronized void doChannelCloseEvent(final String remoteAddr, final Channel channel) {
        if (channel != null) {
            for (var entry : this.groupChannelTable.entrySet()) {
                var group = entry.getKey();
                var clientChannelInfoTable = entry.getValue();
                var clientChannelInfo = clientChannelInfoTable.remove(channel);
                if (clientChannelInfo != null) {
                    clientChannelTable.remove(clientChannelInfo.getClientId());
                    log.info("NETTY EVENT: remove channel[{}][{}] from ProducerManager groupChannelTable, producer group: {}",
                            clientChannelInfo.toString(), remoteAddr, group);
                }

            }
        }
    }

    /**
     * 注册并管理新的producer组客户端
     */
    public synchronized void registerProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
        if (null == channelTable) {
            channelTable = new ConcurrentHashMap<>();
            this.groupChannelTable.put(group, channelTable);
        }

        ClientChannelInfo clientChannelInfoFound = channelTable.get(clientChannelInfo.getChannel());
        if (null == clientChannelInfoFound) {
            channelTable.put(clientChannelInfo.getChannel(), clientChannelInfo);
            clientChannelTable.put(clientChannelInfo.getClientId(), clientChannelInfo.getChannel());
            log.info("new producer connected, group: {} channel: {}", group, clientChannelInfo.toString());
        }

        if (clientChannelInfoFound != null) {
            clientChannelInfoFound.setLastUpdateTimestamp(System.currentTimeMillis());
        }
    }

    /**
     * 移除管理的producer channel
     */
    public synchronized void unregisterProducer(final String group, final ClientChannelInfo clientChannelInfo) {
        ConcurrentHashMap<Channel, ClientChannelInfo> channelTable = this.groupChannelTable.get(group);
        if (null != channelTable && !channelTable.isEmpty()) {
            ClientChannelInfo old = channelTable.remove(clientChannelInfo.getChannel());
            clientChannelTable.remove(clientChannelInfo.getClientId());
            if (old != null) {
                log.info("unregister a producer[{}] from groupChannelTable {}", group,
                        clientChannelInfo.toString());
            }

            if (channelTable.isEmpty()) {
                this.groupChannelTable.remove(group);
                log.info("unregister a producer group[{}] from groupChannelTable", group);
            }
        }
    }

    /**
     * 获取可用producer channel 用于检查事务消息状态
     */
    public Channel getAvailableChannel(String groupId) {
        if (groupId == null) {
            return null;
        }
        List<Channel> channelList;
        ConcurrentHashMap<Channel, ClientChannelInfo> channelClientChannelInfoHashMap = groupChannelTable.get(groupId);
        if (channelClientChannelInfoHashMap != null) {
            channelList = new ArrayList<>(channelClientChannelInfoHashMap.keySet());
        } else {
            log.warn("Check transaction failed, channel table is empty. groupId={}", groupId);
            return null;
        }

        int size = channelList.size();
        if (0 == size) {
            log.warn("Channel list is empty. groupId={}", groupId);
            return null;
        }

        Channel lastActiveChannel = null;

        int index = positiveAtomicCounter.incrementAndGet() % size;
        Channel channel = channelList.get(index);
        int count = 0;
        boolean isOk = channel.isActive() && channel.isWritable();
        while (count++ < GET_AVAILABLE_CHANNEL_RETRY_COUNT) {
            if (isOk) {
                return channel;
            }
            if (channel.isActive()) {
                lastActiveChannel = channel;
            }
            index = (++index) % size;
            channel = channelList.get(index);
            isOk = channel.isActive() && channel.isWritable();
        }

        return lastActiveChannel;
    }

    /**
     * 查询channel
     */
    public Channel findChannel(String clientId) {
        return clientChannelTable.get(clientId);
    }
}
