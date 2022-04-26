package org.apache.rocketmq.store;

import lombok.Getter;

import java.nio.ByteBuffer;

/**
 * 消息查询结果
 */
public class SelectMappedBufferResult {

    /**
     * 消息所在文件逻辑offset
     */
    @Getter
    private final long startOffset;

    /**
     * 消息位置内存映射切片
     */
    @Getter
    private final ByteBuffer byteBuffer;

    /**
     * 消息大小
     */
    @Getter
    private int size;

    private MappedFile mappedFile;

    public SelectMappedBufferResult(long startOffset, ByteBuffer byteBuffer, int size, MappedFile mappedFile) {
        this.startOffset = startOffset;
        this.byteBuffer = byteBuffer;
        this.size = size;
        this.mappedFile = mappedFile;
    }

    public void setSize(final int s) {
        this.size = s;
        this.byteBuffer.limit(this.size);
    }

    @Override
    @Deprecated
    protected void finalize() {
        if (this.mappedFile != null) {
            this.release();
        }
    }

    /**
     * 释放该结果对映射文件的引用
     */
    public synchronized void release() {
        if (this.mappedFile != null) {
            this.mappedFile.release();
            this.mappedFile = null;
        }
    }
}
