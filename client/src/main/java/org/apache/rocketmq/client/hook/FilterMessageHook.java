package org.apache.rocketmq.client.hook;

/**
 * 未使用
 */
public interface FilterMessageHook {
    String hookName();

    void filterMessage(final FilterMessageContext context);
}
