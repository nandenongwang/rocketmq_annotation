package org.apache.rocketmq.common.protocol.heartbeat;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashSet;
import java.util.Set;

/**
 * 客户端心跳数据
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class HeartbeatData extends RemotingSerializable {

    /**
     * 客户端ID
     */
    private String clientID;

    /**
     * 所有生产者信息
     */
    private Set<ProducerData> producerDataSet = new HashSet<>();

    /**
     * 所有消费者信息
     */
    private Set<ConsumerData> consumerDataSet = new HashSet<>();
}
