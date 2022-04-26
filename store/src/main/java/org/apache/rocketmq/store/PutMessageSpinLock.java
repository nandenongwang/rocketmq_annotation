package org.apache.rocketmq.store;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 存储消息自旋锁 【低并发场景使用】
 */
public class PutMessageSpinLock implements PutMessageLock {
    //true: Can lock, false : in lock.
    private final AtomicBoolean putMessageSpinLock = new AtomicBoolean(true);

    @Override
    public void lock() {
        boolean flag;
        do {
            flag = this.putMessageSpinLock.compareAndSet(true, false);
        }
        while (!flag);
    }

    @Override
    public void unlock() {
        this.putMessageSpinLock.compareAndSet(false, true);
    }
}
