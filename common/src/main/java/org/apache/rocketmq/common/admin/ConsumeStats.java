package org.apache.rocketmq.common.admin;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;
import java.util.Map.Entry;

/**
 * 消费者进度状态
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class ConsumeStats extends RemotingSerializable {

    /**
     * consumer对所有queue消费进度
     */
    private HashMap<MessageQueue, OffsetWrapper> offsetTable = new HashMap<>();

    /**
     * consumer消费tps
     */
    private double consumeTps = 0;

    /**
     * 计算所有未消费消息数量
     */
    public long computeTotalDiff() {
        long diffTotal = 0L;

        for (Entry<MessageQueue, OffsetWrapper> next : this.offsetTable.entrySet()) {
            long diff = next.getValue().getBrokerOffset() - next.getValue().getConsumerOffset();
            diffTotal += diff;
        }

        return diffTotal;
    }

}
