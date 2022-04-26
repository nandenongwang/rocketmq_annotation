package org.apache.rocketmq.store;

/**
 * 存储消息状态
 */
public enum PutMessageStatus {

    /**
     * 存储成功
     */
    PUT_OK,

    /**
     * 等待消息刷盘超时
     */
    FLUSH_DISK_TIMEOUT,

    /**
     * 等待消息同步超时
     */
    FLUSH_SLAVE_TIMEOUT,

    /**
     * slave不健康 【同步消息进度过慢】
     */
    SLAVE_NOT_AVAILABLE,

    /**
     * salve角色不支持存储消息 【仅能复制消息数据直接追加到日志中】
     */
    SERVICE_NOT_AVAILABLE,

    /**
     * 创建映射文件失败 【磁盘故障等】
     */
    CREATE_MAPEDFILE_FAILED,

    /**
     * 消息属性约束异常
     */
    MESSAGE_ILLEGAL,

    /**
     * 消息properties过大
     */
    PROPERTIES_SIZE_EXCEEDED,

    /**
     * 页缓存写入繁忙
     */
    OS_PAGECACHE_BUSY,

    /**
     * 未知错误
     */
    UNKNOWN_ERROR,
}
