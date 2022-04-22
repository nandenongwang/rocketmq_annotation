package org.apache.rocketmq.client.hook;

import org.apache.rocketmq.client.exception.MQClientException;

/**
 * 未使用
 */
public interface CheckForbiddenHook {
    String hookName();

    void checkForbidden(final CheckForbiddenContext context) throws MQClientException;
}
