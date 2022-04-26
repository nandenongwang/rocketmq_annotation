package org.apache.rocketmq.store;

/**
 * 消息拉取状态
 */
public enum GetMessageStatus {

    /**
     * 拉取到新消息
     */
    FOUND,

    /**
     * 找不到匹配的消息
     */
    NO_MATCHED_MESSAGE,

    /**
     * 找不到offset位置消费索引对应的消息 【可能commitlog被删除】
     */
    MESSAGE_WAS_REMOVING,

    /**
     * 找不到offset对应的消费索引文件
     */
    OFFSET_FOUND_NULL,

    /**
     * 拉取offset过大很多
     */
    OFFSET_OVERFLOW_BADLY,

    /**
     * 拉取offset过大
     */
    OFFSET_OVERFLOW_ONE,

    /**
     * 拉取offset过小 【commitlog已被删除】
     */
    OFFSET_TOO_SMALL,

    /**
     * 找不到拉取的queueId
     */
    NO_MATCHED_LOGIC_QUEUE,

    /**
     * queue中没有索引项 【没有消息或reput服务dispatch过慢】
     */
    NO_MESSAGE_IN_QUEUE,
}
