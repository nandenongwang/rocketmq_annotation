package org.apache.rocketmq.common;

/**
 * 授时服务
 */
public class SystemClock {
    public long now() {
        return System.currentTimeMillis();
    }
}
