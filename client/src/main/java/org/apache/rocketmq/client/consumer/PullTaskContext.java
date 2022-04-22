package org.apache.rocketmq.client.consumer;

public class PullTaskContext {

    private int pullNextDelayTimeMillis = 200;

    private MQPullConsumer pullConsumer;

    public int getPullNextDelayTimeMillis() {
        return pullNextDelayTimeMillis;
    }

    public void setPullNextDelayTimeMillis(int pullNextDelayTimeMillis) {
        this.pullNextDelayTimeMillis = pullNextDelayTimeMillis;
    }

    public MQPullConsumer getPullConsumer() {
        return pullConsumer;
    }

    public void setPullConsumer(MQPullConsumer pullConsumer) {
        this.pullConsumer = pullConsumer;
    }
}
