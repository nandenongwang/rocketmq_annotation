package org.apache.rocketmq.client.consumer;

import lombok.Data;

@Data
public class PullTaskContext {

    private int pullNextDelayTimeMillis = 200;

    private MQPullConsumer pullConsumer;
}
