package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

@NoArgsConstructor
@Data
public class GetMessageResult {

    private final List<SelectMappedBufferResult> messageMapedList = new ArrayList<>(100);

    private final List<ByteBuffer> messageBufferList = new ArrayList<>(100);
    private GetMessageStatus status;
    private long nextBeginOffset;
    private long minOffset;
    private long maxOffset;
    private int bufferTotalSize = 0;
    private boolean suggestPullingFromSlave = false;
    private int msgCount4Commercial = 0;

    public void addMessage(final SelectMappedBufferResult mapedBuffer) {
        this.messageMapedList.add(mapedBuffer);
        this.messageBufferList.add(mapedBuffer.getByteBuffer());
        this.bufferTotalSize += mapedBuffer.getSize();
        this.msgCount4Commercial += (int) Math.ceil(
                mapedBuffer.getSize() / BrokerStatsManager.SIZE_PER_COUNT);
    }

    public void release() {
        for (SelectMappedBufferResult select : this.messageMapedList) {
            select.release();
        }
    }

    public int getMessageCount() {
        return this.messageMapedList.size();
    }

}
