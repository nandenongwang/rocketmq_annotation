package org.apache.rocketmq.common.protocol.heartbeat;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum ConsumeType {

    CONSUME_ACTIVELY("PULL"),

    CONSUME_PASSIVELY("PUSH");

    @Getter
    private String typeCN;
}
