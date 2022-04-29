package org.apache.rocketmq.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 线程工厂
 */
public class ThreadFactoryImpl implements ThreadFactory {

    /**
     * 线程号
     */
    private final AtomicLong threadIndex = new AtomicLong(0);

    /**
     * 线程名前缀
     */
    private final String threadNamePrefix;

    /**
     * 是否创建守护线程
     */
    private final boolean daemon;

    public ThreadFactoryImpl(final String threadNamePrefix) {
        this(threadNamePrefix, false);
    }

    public ThreadFactoryImpl(final String threadNamePrefix, boolean daemon) {
        this.threadNamePrefix = threadNamePrefix;
        this.daemon = daemon;
    }

    /**
     * 创建指定线程名线程、并设置daemon
     */
    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());
        thread.setDaemon(daemon);
        return thread;
    }
}
