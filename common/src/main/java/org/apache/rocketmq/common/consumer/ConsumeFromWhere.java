package org.apache.rocketmq.common.consumer;

/**
 * 起始消费策略
 */
public enum ConsumeFromWhere {

    /**
     * push:
     * 从消费进度开始 【没有进度时、正常从最后一条开始、重试从第一条开始】
     * pull:
     * litepull:
     */
    CONSUME_FROM_LAST_OFFSET,

    /**
     * push:
     * 从消费进度开始 【没有进度时、正常&重试都从第一条开始】
     * pull:
     * litepull:
     */
    CONSUME_FROM_FIRST_OFFSET,

    /**
     * push:
     * 从消费进度开始 【没有进度时、重试冲最后一条开始、正常从启动前半小时存储的消息开始】
     * pull:
     * litepull:
     */
    CONSUME_FROM_TIMESTAMP,

    @Deprecated
    CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST,
    @Deprecated
    CONSUME_FROM_MIN_OFFSET,
    @Deprecated
    CONSUME_FROM_MAX_OFFSET;
}
