package org.apache.rocketmq.broker.pagecache;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import org.apache.rocketmq.store.SelectMappedBufferResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

/**
 * 零拷贝传输单个消息结果
 */
public class OneMessageTransfer extends AbstractReferenceCounted implements FileRegion {

    /**
     * 响应头、非零拷贝传输
     */
    private final ByteBuffer byteBufferHeader;

    /**
     * 日志文件内存映射消息切片
     */
    private final SelectMappedBufferResult selectMappedBufferResult;

    /**
     * Bytes which were transferred already.
     */
    private long transferred;

    public OneMessageTransfer(ByteBuffer byteBufferHeader, SelectMappedBufferResult selectMappedBufferResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.selectMappedBufferResult = selectMappedBufferResult;
    }


    /**
     * 文件传输起始offset、因先传输了响应头、一轮输出内容作为一个逻辑文件因加上响应头大小
     */
    @Override
    public long position() {
        return this.byteBufferHeader.position() + this.selectMappedBufferResult.getByteBuffer().position();
    }

    @Override
    public long transfered() {
        return transferred;
    }

    /**
     * 总共需写出响应头 + 消息内容大小数据
     */
    @Override
    public long count() {
        return this.byteBufferHeader.limit() + this.selectMappedBufferResult.getSize();
    }

    /**
     * 首次堆拷贝输出响应头、未达到总大小再次零拷贝输出消息内容
     */
    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transferred += target.write(this.byteBufferHeader);
            return transferred;
        } else if (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
            transferred += target.write(this.selectMappedBufferResult.getByteBuffer());
            return transferred;
        }
        return 0;
    }

    public void close() {
        this.deallocate();
    }

    @Override
    protected void deallocate() {
        this.selectMappedBufferResult.release();
    }
}
