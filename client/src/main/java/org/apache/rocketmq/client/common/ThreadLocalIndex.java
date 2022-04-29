package org.apache.rocketmq.client.common;

import java.util.Random;

/**
 * 线程独占递增随机数
 */
public class ThreadLocalIndex {
    private final ThreadLocal<Integer> threadLocalIndex = new ThreadLocal<>();
    private final Random random = new Random();

    public int getAndIncrement() {
        Integer index = this.threadLocalIndex.get();
        if (null == index) {
            index = Math.abs(random.nextInt());
            this.threadLocalIndex.set(index);
        }

        index = Math.abs(index + 1);
        this.threadLocalIndex.set(index);
        return index;
    }

    @Override
    public String toString() {
        return "ThreadLocalIndex{" +
                "threadLocalIndex=" + threadLocalIndex.get() +
                '}';
    }
}
