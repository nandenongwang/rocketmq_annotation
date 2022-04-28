package org.apache.rocketmq.broker.pagecache;

import io.netty.channel.FileRegion;
import io.netty.util.AbstractReferenceCounted;
import org.apache.rocketmq.store.QueryMessageResult;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;

/**
 * 零拷贝传输查询消息结果
 */
public class QueryMessageTransfer extends AbstractReferenceCounted implements FileRegion {
    private final ByteBuffer byteBufferHeader;
    private final QueryMessageResult queryMessageResult;

    /**
     * Bytes which were transferred already.
     */
    private long transferred;

    public QueryMessageTransfer(ByteBuffer byteBufferHeader, QueryMessageResult queryMessageResult) {
        this.byteBufferHeader = byteBufferHeader;
        this.queryMessageResult = queryMessageResult;
    }

    @Override
    public long position() {
        int pos = byteBufferHeader.position();
        List<ByteBuffer> messageBufferList = this.queryMessageResult.getMessageBufferList();
        for (ByteBuffer bb : messageBufferList) {
            pos += bb.position();
        }
        return pos;
    }

    @Override
    public long transfered() {
        return transferred;
    }

    @Override
    public long count() {
        return byteBufferHeader.limit() + this.queryMessageResult.getBufferTotalSize();
    }

    @Override
    public long transferTo(WritableByteChannel target, long position) throws IOException {
        if (this.byteBufferHeader.hasRemaining()) {
            transferred += target.write(this.byteBufferHeader);
            return transferred;
        } else {
            List<ByteBuffer> messageBufferList = this.queryMessageResult.getMessageBufferList();
            for (ByteBuffer bb : messageBufferList) {
                if (bb.hasRemaining()) {
                    transferred += target.write(bb);
                    return transferred;
                }
            }
        }

        return 0;
    }

    public void close() {
        this.deallocate();
    }

    @Override
    protected void deallocate() {
        this.queryMessageResult.release();
    }
}
