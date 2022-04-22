package org.apache.rocketmq.common.consumer;

/**
 * 起始消费策略
 */
public enum ConsumeFromWhere {

    /**
     * 从上次提交offset开始消费
     */
    CONSUME_FROM_LAST_OFFSET,

    /**
     *
     */
    CONSUME_FROM_FIRST_OFFSET,

    /**
     *
     */
    CONSUME_FROM_TIMESTAMP,

    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    @Deprecated
    CONSUME_FROM_MIN_OFFSET,
    @Deprecated
    CONSUME_FROM_MAX_OFFSET;
}
