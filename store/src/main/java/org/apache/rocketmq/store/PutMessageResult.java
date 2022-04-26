package org.apache.rocketmq.store;

import lombok.Data;

/**
 * 存储消息结果
 */
@Data
public class PutMessageResult {

    /**
     * 存储消息状态
     */
    private PutMessageStatus putMessageStatus;

    /**
     * 写入消息结果
     */
    private AppendMessageResult appendMessageResult;

    public PutMessageResult(PutMessageStatus putMessageStatus, AppendMessageResult appendMessageResult) {
        this.putMessageStatus = putMessageStatus;
        this.appendMessageResult = appendMessageResult;
    }

    public boolean isOk() {
        return this.appendMessageResult != null && this.appendMessageResult.isOk();
    }

}
