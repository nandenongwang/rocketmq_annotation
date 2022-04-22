package org.apache.rocketmq.client.consumer;

/**
 * Async message pulling interface
 */
public interface PullCallback {
    void onSuccess(final PullResult pullResult);

    void onException(final Throwable e);
}
