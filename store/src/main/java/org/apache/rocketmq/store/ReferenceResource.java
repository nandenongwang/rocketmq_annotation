package org.apache.rocketmq.store;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 引用资源
 */
public abstract class ReferenceResource {

    /**
     * 被引用次数
     */
    protected final AtomicLong refCount = new AtomicLong(1);

    /**
     * 是否可用
     */
    @Getter
    protected volatile boolean available = true;

    /**
     * 是否被清理
     */
    protected volatile boolean cleanupOver = false;

    /**
     * 首次关闭时间
     */
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 增加引用
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    /**
     * 关闭资源
     */
    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 释放引用
     */
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0) {
            return;
        }

        synchronized (this) {

            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
