package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.common.message.MessageQueue;

public interface PullTaskCallback {
    void doPullTask(final MessageQueue mq, final PullTaskContext context);
}
