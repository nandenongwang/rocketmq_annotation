package org.apache.rocketmq.client.consumer.store;

/**
 * 从哪里读取offset
 */
public enum ReadOffsetType {
    /**
     * 本地内存
     */
    READ_FROM_MEMORY,
    /**
     * broker
     */
    READ_FROM_STORE,
    /**
     * 内存优先然后持久化存储 本地文件或远程broker
     */
    MEMORY_FIRST_THEN_STORE;
}
