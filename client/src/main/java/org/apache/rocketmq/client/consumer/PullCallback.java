package org.apache.rocketmq.client.consumer;

/**
 * 拉取消息回调
 */
public interface PullCallback {
    void onSuccess(final PullResult pullResult);

    void onException(final Throwable e);
}
