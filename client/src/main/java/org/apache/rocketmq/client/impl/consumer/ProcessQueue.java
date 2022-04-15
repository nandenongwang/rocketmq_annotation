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
package org.apache.rocketmq.client.impl.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * Queue consumption snapshot
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();

    /**
     * 缓存tree 锁
     */
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();

    /**
     * 缓存待消费的消息tree
     */
    @Getter
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<>();

    /**
     * 缓存消息数
     */
    @Getter
    private final AtomicLong msgCount = new AtomicLong();

    /**
     * 缓存消息body总大小
     */
    @Getter
    private final AtomicLong msgSize = new AtomicLong();


    /**
     * 拉取到的消息中最大的offset
     */
    private volatile long queueOffsetMax = 0L;

    /**
     * 丢弃该processqueue标识
     */
    @Setter
    @Getter
    private volatile boolean dropped = false;

    /**
     * 上次拉取消息时间
     */
    @Getter
    @Setter
    private volatile long lastPullTimestamp = System.currentTimeMillis();

    /**
     * 上次消费消息时间【从缓存tree中取出消息】
     */
    @Getter
    @Setter
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();

    /**
     * 消费中标识 【消费任务取不到消息时 false 新消息加入缓存消息时 true】
     */
    @Getter
    @Setter
    private volatile boolean consuming = false;

    /**
     * 拉取落后进度 【brokerqueue最大offset - 缓存最大offset】
     */

    @Getter
    @Setter
    private volatile long msgAccCnt = 0;

    //region 顺序消息字段
    /**
     * 缓存正在消费的顺序消息tree 仅顺序消息使用
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<>();

    /**
     * 顺序消息全局锁
     */
    @Getter
    @Setter
    private volatile boolean locked = false;

    /**
     * 全局锁最近加锁时间
     */
    @Getter
    @Setter
    private volatile long lastLockTimestamp = System.currentTimeMillis();

    /**
     * 顺序消息本地锁 【确保多线程下单个queue同时仅有单个线程消费】
     */
    @Getter
    private final Lock lockConsume = new ReentrantLock();

    /**
     * 尝试获取顺序消息本地锁失败次数
     */
    @Getter
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    //endregion

    /**
     * 全局锁是否过期 【上次全局锁加锁时间是否超过默认30s】
     */
    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    /**
     * pull是否过期 【默认2分钟都没pull过了】
     */
    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 清理过期消息
     * 最大清理16个超时还未消费消息 将该消息重新投递到该queue的延时重试队列中 并移除该消息
     * 并发消息移除后提交了移除的消息会在延迟队列中等待重试 不会因为之后offset提交而连带丢失
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        //region 顺序消费不能清理过期消息 必须等待一个一个消费完成
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }
        //endregion

        int loop = Math.min(msgTreeMap.size(), 16);
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                this.lockTreeMap.readLock().lockInterruptibly();
                try {
                    if (!msgTreeMap.isEmpty() && System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue())) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                        msg = msgTreeMap.firstEntry().getValue();
                    } else {
                        break;
                    }
                } finally {
                    this.lockTreeMap.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {
                assert msg != null;
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                try {
                    this.lockTreeMap.writeLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        this.lockTreeMap.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 将拉取到的消息放入processqueue缓存中
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        boolean dispatchToConsume = false;
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {

                    //region 将消息存入缓存tree中
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    //endregion

                    //region 拉取到新消息 增加缓存消息总数 & queue最大offset & 缓存消息总大小
                    if (null == old) {

                        //region 增加缓存消息总数
                        msgCount.addAndGet(1);
                        //endregion

                        //region更新queue最大offset  新消息的offset必然比所有旧消息大
                        this.queueOffsetMax = msg.getQueueOffset();
                        //endregion

                        //region 增加缓存消息总大小
                        msgSize.addAndGet(msg.getBody().length);
                        //endregion
                    }
                    //endregion
                }

                //region 若存在缓存消息且消费中标识 false 更新消费中标识
                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    dispatchToConsume = true;
                    this.consuming = true;
                }
                //endregion

                //region 更新该queue的拉取落后进度
                if (!msgs.isEmpty()) {
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    if (property != null) {
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
                //endregion

            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    /**
     * 获取缓存消息最大跨度 缓存tree中 最大offset - 最小offset
     */
    public long getMaxSpan() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * 消费成功后移除消息 返回最小未被消费的offset用以提交消费进度
     */
    public long removeMessage(List<MessageExt> msgs) {
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                //region 缓存消息tree挨个移除消息 更新缓存消息数 & 缓存消息总大小 & 可提交offset
                if (!msgTreeMap.isEmpty()) {
                    for (MessageExt msg : msgs) {
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        if (prev != null) {
                            msgCount.addAndGet(-1);
                            msgSize.addAndGet(-msg.getBody().length);
                        }
                    }

                    //region 返回最小offset提交进度
                    if (!msgTreeMap.isEmpty()) {
                        result = msgTreeMap.firstKey();
                    }
                    //endregion
                }
                //endregion

                //region 缓存消息tree被消费空 提交最大offset
                if (msgTreeMap.isEmpty()) {
                    result = this.queueOffsetMax + 1;//todo 为何 +1
                }
                //endregion
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    /**
     * 回滚所有顺序消费消息 【将正在消费的顺序消息tree全加入消息tree中 并清空顺序消息tree 】
     */
    public void rollback() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 回滚一批顺序消费消息 【清理顺序消费tree 重新加入缓存消息tree】
     */
    public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    /**
     * 提交正在消费中顺序消息 【顺序消费tree中lastoffset作为提交offset】
     * 清空顺序消息消费tree 修改processqueue统计值
     */
    public long commit() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                msgCount.addAndGet(-this.consumingMsgOrderlyTreeMap.size());
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    msgSize.addAndGet(-msg.getBody().length);
                }
                this.consumingMsgOrderlyTreeMap.clear();
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    /**
     * 从缓存中批量获取消息消费 【同时放入缓存消费tree中】
     */
    public List<MessageExt> takeMessages(final int batchSize) {
        List<MessageExt> result = new ArrayList<>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            result.add(entry.getValue());
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }

                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    /**
     * 是否有缓存消息
     */
    public boolean hasTempMessage() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException ignored) {
        }

        return true;
    }

    /**
     * 清空重置 processqueue
     */
    public void clear() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 增加顺序消息本地锁尝试获取次数 【销毁该order processqueue时需确保没人获取到本地锁正在消费中】
     */
    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    /**
     * 获取processqueue信息 【填充pull快照】
     */
    public void fillProcessQueueInfo(ProcessQueueInfo info) {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception ignored) {
        } finally {
            this.lockTreeMap.readLock().unlock();
        }
    }

}
