package org.apache.rocketmq.store;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 消息过滤器 用于服务端消息过滤
 */
public interface MessageFilter {
    /**
     * 粗筛 【使用consumequeue ext中消息扩展部的计算后位图筛选掉100%不满足的消息】
     */
    boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit);

    /**
     * 精筛 【直接取出粗晒后剩余的少量的消息properties重新计算】
     */
    boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties);
}
