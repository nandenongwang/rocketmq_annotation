package org.apache.rocketmq.store;

/**
 * 写入消息状态码
 */
public enum AppendMessageStatus {

    /**
     * 写入成功
     */
    PUT_OK,

    /**
     * 剩余空间不足 【写入失败】
     */
    END_OF_FILE,

    /**
     * 消息长度过大 【写入失败】
     */
    MESSAGE_SIZE_EXCEEDED,

    /**
     * 消息properties长度过大 【写入失败】
     */
    PROPERTIES_SIZE_EXCEEDED,

    /**
     * 未知错误 【写入失败】
     */
    UNKNOWN_ERROR,
}
