package org.apache.rocketmq.common.admin;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;

@EqualsAndHashCode(callSuper = false)
@Data
public class TopicStatsTable extends RemotingSerializable {
    private HashMap<MessageQueue, TopicOffset> offsetTable = new HashMap<>();
}
