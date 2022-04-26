package org.apache.rocketmq.store;

import org.apache.rocketmq.common.message.MessageExtBatch;

import java.nio.ByteBuffer;

/**
 * 写入消息回调 【按存储协议编码并写入页缓存中】
 */
public interface AppendMessageCallback {

    /**
     * 写入单条消息
     */
    AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, MessageExtBrokerInner msg);

    /**
     * 写入批量消息
     */
    AppendMessageResult doAppend(long fileFromOffset, ByteBuffer byteBuffer, int maxBlank, MessageExtBatch messageExtBatch);
}
