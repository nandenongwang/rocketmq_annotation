package org.apache.rocketmq.store;

import lombok.Data;

/**
 * 写入消息结果
 */
@Data
public class AppendMessageResult {

    /**
     * 写入结果状态码
     */
    private AppendMessageStatus status;

    /**
     * 开始写入位置
     */
    private long wroteOffset;

    /**
     * 写入消息大小 【当写入空消息时为剩余空间大小】
     */
    private int wroteBytes;

    /**
     * 消息ID 【主机地址 + 写入位置】
     */
    private String msgId;

    /**
     * 消息存储时间
     */
    private long storeTimestamp;

    /**
     * 消费索引写入位置 【consumequeue索引文件逻辑offset】
     */
    private long logicsOffset;

    /**
     * 本次写入操作页缓存耗时
     */
    private long pagecacheRT = 0;

    /**
     * 写入消息数量
     */
    private int msgNum = 1;

    public AppendMessageResult(AppendMessageStatus status) {
        this(status, 0, 0, "", 0, 0, 0);
    }

    public AppendMessageResult(AppendMessageStatus status, long wroteOffset, int wroteBytes, String msgId, long storeTimestamp, long logicsOffset, long pagecacheRT) {
        this.status = status;
        this.wroteOffset = wroteOffset;
        this.wroteBytes = wroteBytes;
        this.msgId = msgId;
        this.storeTimestamp = storeTimestamp;
        this.logicsOffset = logicsOffset;
        this.pagecacheRT = pagecacheRT;
    }

    /**
     * 本次写入是否成功
     */
    public boolean isOk() {
        return this.status == AppendMessageStatus.PUT_OK;
    }

}
