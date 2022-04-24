package org.apache.rocketmq.store.config;

/**
 * 刷盘方式
 */
public enum FlushDiskType {

    /**
     * 同步刷盘
     */
    SYNC_FLUSH,

    /**
     * 异步刷盘
     */
    ASYNC_FLUSH
}
