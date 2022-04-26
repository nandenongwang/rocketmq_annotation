package org.apache.rocketmq.store;

import java.util.concurrent.locks.ReentrantLock;

/**
 * 存储消息独占锁 【普通重入锁、默认使用】
 */
public class PutMessageReentrantLock implements PutMessageLock {
    private final ReentrantLock putMessageNormalLock = new ReentrantLock(); // NonfairSync

    @Override
    public void lock() {
        putMessageNormalLock.lock();
    }

    @Override
    public void unlock() {
        putMessageNormalLock.unlock();
    }
}
