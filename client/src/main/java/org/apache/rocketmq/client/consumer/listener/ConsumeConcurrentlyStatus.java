package org.apache.rocketmq.client.consumer.listener;

/**
 * 业务消费返回消费状态
 * 成功、重试 【返回null也是重试】
 */
public enum ConsumeConcurrentlyStatus {
    /**
     * Success consumption
     */
    CONSUME_SUCCESS,
    /**
     * Failure consumption,later try to consume
     */
    RECONSUME_LATER;
}
