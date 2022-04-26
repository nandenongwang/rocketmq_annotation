package org.apache.rocketmq.store;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * 消息查询结果
 */
@NoArgsConstructor
@Data
public class GetMessageResult {

    /**
     * 拉取到所有消息
     */
    private final List<SelectMappedBufferResult> messageMapedList = new ArrayList<>(100);
    private final List<ByteBuffer> messageBufferList = new ArrayList<>(100);

    /**
     * 消息拉取状态
     */
    private GetMessageStatus status;

    /**
     * 下次拉取位置 【消费索引文件逻辑offset】
     */
    private long nextBeginOffset;

    /**
     * 本次拉取最小offset
     */
    private long minOffset;

    /**
     * 本次拉取最大offset
     */
    private long maxOffset;

    /**
     * 所有消息大小
     */
    private int bufferTotalSize = 0;

    /**
     * 是否建议下次从salve拉取
     */
    private boolean suggestPullingFromSlave = false;
    private int msgCount4Commercial = 0;

    /**
     * 增加结果集消息
     */
    public void addMessage(SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(mapedBuffer.getSize() / BrokerStatsManager.SIZE_PER_COUNT);
    }

    /**
     * 释放资源引用
     */
    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    /**
     * 拉取到消息总数
     */
    public int getMessageCount() {
        return this.messageMapedList.size();
    }

}
