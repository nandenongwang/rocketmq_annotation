package org.apache.rocketmq.common.admin;

import lombok.Data;

/**
 *
 */
@Data
public class RollbackStats {
    private String brokerName;
    private long queueId;
    private long brokerOffset;
    private long consumerOffset;
    private long timestampOffset;
    private long rollbackOffset;
}
