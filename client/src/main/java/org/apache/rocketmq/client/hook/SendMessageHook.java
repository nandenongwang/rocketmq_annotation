package org.apache.rocketmq.client.hook;

/**
 * 发送消息hook
 */
public interface SendMessageHook {
    String hookName();

    void sendMessageBefore(final SendMessageContext context);

    void sendMessageAfter(final SendMessageContext context);
}
