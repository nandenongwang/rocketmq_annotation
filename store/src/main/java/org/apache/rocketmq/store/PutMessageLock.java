package org.apache.rocketmq.store;

/**
 * 存储消息锁
 */
public interface PutMessageLock {
    void lock();

    void unlock();
}
