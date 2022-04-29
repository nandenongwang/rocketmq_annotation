package org.apache.rocketmq.common.admin;

import lombok.Data;

@Data
public class TopicOffset {
    private long minOffset;
    private long maxOffset;
    private long lastUpdateTimestamp;
}
