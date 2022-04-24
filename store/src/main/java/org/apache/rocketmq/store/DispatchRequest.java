package org.apache.rocketmq.store;

import lombok.Data;

import java.util.Map;

/**
 * 消息分派请求
 */
@Data
public class DispatchRequest {

    /**
     * 消息topic
     */
    private final String topic;

    /**
     * 要写入的consumequeueId
     */
    private final int queueId;

    /**
     * commitlog位置
     */
    private final long commitLogOffset;

    /**
     * 消息总大小
     */
    private int msgSize;

    /**
     * 消息tags哈希码
     */
    private final long tagsCode;

    /**
     * 消息存储时间
     */
    private final long storeTimestamp;

    /**
     * consumequeue位置
     */
    private final long consumeQueueOffset;

    /**
     * 消息构建索引keys
     */
    private final String keys;

    /**
     * 分派器处理结果
     */
    private final boolean success;

    /**
     * 客户端生成唯一消息ID
     */
    private final String uniqKey;

    /**
     * sysFlag 【pull提交、事务标识等】
     */
    private final int sysFlag;

    /**
     * half消息commitlog位置 【与commitLogOffset相同】
     */
    private final long preparedTransactionOffset;

    /**
     * 消息properties
     */
    private final Map<String, String> propertiesMap;

    /**
     * 消息满足订阅条件消费组位图
     */
    private byte[] bitMap;


    private int bufferSize = -1;//the buffer size maybe larger than the msg size if the message is wrapped by something

    public DispatchRequest(String topic, int queueId, long commitLogOffset, int msgSize, long tagsCode, long storeTimestamp, long consumeQueueOffset,
                           String keys, String uniqKey, int sysFlag, long preparedTransactionOffset, Map<String, String> propertiesMap) {
        this.topic = topic;
        this.queueId = queueId;
        this.commitLogOffset = commitLogOffset;
        this.msgSize = msgSize;
        this.tagsCode = tagsCode;
        this.storeTimestamp = storeTimestamp;
        this.consumeQueueOffset = consumeQueueOffset;
        this.keys = keys;
        this.uniqKey = uniqKey;

        this.sysFlag = sysFlag;
        this.preparedTransactionOffset = preparedTransactionOffset;
        this.success = true;
        this.propertiesMap = propertiesMap;
    }

    public DispatchRequest(int size) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = false;
        this.propertiesMap = null;
    }

    public DispatchRequest(int size, boolean success) {
        this.topic = "";
        this.queueId = 0;
        this.commitLogOffset = 0;
        this.msgSize = size;
        this.tagsCode = 0;
        this.storeTimestamp = 0;
        this.consumeQueueOffset = 0;
        this.keys = "";
        this.uniqKey = null;
        this.sysFlag = 0;
        this.preparedTransactionOffset = 0;
        this.success = success;
        this.propertiesMap = null;
    }
}
