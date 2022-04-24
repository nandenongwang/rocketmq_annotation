package org.apache.rocketmq.store;

import java.util.Map;

/**
 * 新消息到达监听器 【用于唤醒长轮询挂起的pull请求】
 */
public interface MessageArrivingListener {
    void arriving(String topic, int queueId, long logicOffset, long tagsCode, long msgStoreTime, byte[] filterBitMap, Map<String, String> properties);
}
