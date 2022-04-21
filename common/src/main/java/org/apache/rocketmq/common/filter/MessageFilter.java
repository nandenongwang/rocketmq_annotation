package org.apache.rocketmq.common.filter;

import org.apache.rocketmq.common.message.MessageExt;

/**
 * 未使用
 */
public interface MessageFilter {
    boolean match(final MessageExt msg, final FilterContext context);
}
