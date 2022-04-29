package org.apache.rocketmq.common.utils;

import java.util.UUID;


/**
 * RPC消息请求ID工具
 */
public class CorrelationIdUtil {

    /**
     * 使用UUID生成新的请求ID
     */
    public static String createCorrelationId() {
        return UUID.randomUUID().toString();
    }
}
