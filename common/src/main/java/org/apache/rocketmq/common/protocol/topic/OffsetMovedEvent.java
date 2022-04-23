package org.apache.rocketmq.common.protocol.topic;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

@EqualsAndHashCode(callSuper = false)
@Data
public class OffsetMovedEvent extends RemotingSerializable {
    /**
     *
     */
    private String consumerGroup;

    /**
     *
     */
    private MessageQueue messageQueue;

    /**
     *
     */
    private long offsetRequest;

    /**
     *
     */
    private long offsetNew;
}
