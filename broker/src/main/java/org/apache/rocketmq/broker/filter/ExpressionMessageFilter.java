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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 表达式消息过滤器
 */
public class ExpressionMessageFilter implements MessageFilter {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    /**
     * 订阅配置
     */
    protected final SubscriptionData subscriptionData;

    /**
     * 过滤数据 topic@consumegroup的字节数组【作为布隆过滤器中存储单位】
     */
    protected final ConsumerFilterData consumerFilterData;

    /**
     * 全局过滤管理器
     */
    protected final ConsumerFilterManager consumerFilterManager;

    /**
     * 过滤数据是否是过滤管理器中布隆过滤器的存储单位
     */
    protected final boolean bloomDataValid;

    public ExpressionMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData, ConsumerFilterManager consumerFilterManager) {
        this.subscriptionData = subscriptionData;
        this.consumerFilterData = consumerFilterData;
        this.consumerFilterManager = consumerFilterManager;
        if (consumerFilterData == null) {
            bloomDataValid = false;
            return;
        }
        BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
        bloomDataValid = bloomFilter != null && bloomFilter.isValid(consumerFilterData.getBloomFilterData());
    }

    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        //消费者没配置订阅表示可消费所有消息
        if (null == subscriptionData) {
            return true;
        }

        //classfilter弃用 默认true
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        // by tags code.
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            //消息没有tag表示可被所有订阅消费
            if (tagsCode == null) {
                return true;
            }

            //消费者订阅了所有tags或者订阅tags中包含该消息tag
            return subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)
                    || subscriptionData.getCodeSet().contains(tagsCode.intValue());
        } else {
            // no expression or no bloom
            if (consumerFilterData == null || consumerFilterData.getExpression() == null
                    || consumerFilterData.getCompiledExpression() == null || consumerFilterData.getBloomFilterData() == null) {
                return true;
            }

            // message is before consumer
            if (cqExtUnit == null || !consumerFilterData.isMsgInLive(cqExtUnit.getMsgStoreTime())) {
                log.debug("Pull matched because not in live: {}, {}", consumerFilterData, cqExtUnit);
                return true;
            }
            //如未开启扩展消息 则不进行表达式匹配 【】

            //从消息扩展部分取出消息满足订阅表达式的所有消费组
            byte[] filterBitMap = cqExtUnit.getFilterBitMap();
            BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
            if (filterBitMap == null || !this.bloomDataValid
                    || filterBitMap.length * Byte.SIZE != consumerFilterData.getBloomFilterData().getBitNum()) {
                return true;
            }

            //判断消费者拉取的 topic@consumegroup 是否在过滤器中进行粗筛 【仅保证不在的100%false 在的需从commitlog中取出消息properties完整匹配】
            BitsArray bitsArray = null;
            try {
                bitsArray = BitsArray.create(filterBitMap);
                boolean ret = bloomFilter.isHit(consumerFilterData.getBloomFilterData(), bitsArray);
                log.debug("Pull {} by bit map:{}, {}, {}", ret, consumerFilterData, bitsArray, cqExtUnit);
                return ret;
            } catch (Throwable e) {
                log.error("bloom filter error, sub=" + subscriptionData
                        + ", filter=" + consumerFilterData + ", bitMap=" + bitsArray, e);
            }
        }

        return true;
    }

    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        //region 同上
        if (subscriptionData == null) {
            return true;
        }

        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }
        //endregion

        //region 重新计算properties是否满足表达式
        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;

        // no expression
        if (realFilterData == null || realFilterData.getExpression() == null
                || realFilterData.getCompiledExpression() == null) {
            return true;
        }


        if (tempProperties == null && msgBuffer != null) {
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }

        Object ret = null;
        try {
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);

            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }

        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);

        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }
        //endregion

        return (Boolean) ret;
    }

}
