package org.apache.rocketmq.common.protocol.heartbeat;

import lombok.Data;

/**
 * 心跳生产者信息
 */
@Data
public class ProducerData {

    /**
     * 生产组名
     */
    private String groupName;
}
