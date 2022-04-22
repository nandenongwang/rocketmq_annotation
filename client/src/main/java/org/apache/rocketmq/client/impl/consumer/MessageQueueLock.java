package org.apache.rocketmq.client.impl.consumer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 顺序消费本地锁 确保单个queue仅有获取到对应锁对象Object的线程能消费这个queue
 */
public class MessageQueueLock {
    private final ConcurrentMap<MessageQueue, Object> mqLockTable = new ConcurrentHashMap<>();

    public Object fetchLockObject(final MessageQueue mq) {
        Object objLock = this.mqLockTable.get(mq);
        if (null == objLock) {
            objLock = new Object();
            Object prevLock = this.mqLockTable.putIfAbsent(mq, objLock);
            if (prevLock != null) {
                objLock = prevLock;
            }
        }

        return objLock;
    }
}
