package org.apache.rocketmq.common;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 后台服务线程
 */
@NoArgsConstructor
public abstract class ServiceThread implements Runnable {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 系统停止时、主线程等待该后台服务线程关闭最大时间
     */
    @Getter
    private static final long JOIN_TIME = 90 * 1000;

    /**
     * 服务执行线程
     */
    private Thread thread;

    /**
     * 线程调度CountDownLatch
     */
    protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);

    /**
     * 服务是否被唤醒
     */
    protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);

    /**
     * 是否停止该后台服务
     */
    @Getter
    protected volatile boolean stopped = false;

    /**
     * 执行是否是守护线程
     */
    @Getter
    @Setter
    protected boolean isDaemon = false;

    /**
     * 后台服务是否启动
     * Make it able to restart the thread
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    /**
     * 获取后台服务名
     */
    public abstract String getServiceName();

    /**
     * 创建线程、根据子类run方法启动后台服务线程
     */
    public void start() {
        log.info("Try to start service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(false, true)) {
            return;
        }
        stopped = false;
        this.thread = new Thread(this, getServiceName());
        this.thread.setDaemon(isDaemon);
        this.thread.start();
    }

    public void shutdown() {
        this.shutdown(false);
    }

    public void shutdown(boolean interrupt) {
        log.info("Try to shutdown service thread:{} started:{} lastThread:{}", getServiceName(), started.get(), thread);
        if (!started.compareAndSet(true, false)) {
            return;
        }
        this.stopped = true;
        log.info("shutdown thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        try {
            if (interrupt) {
                this.thread.interrupt();
            }

            long beginTime = System.currentTimeMillis();
            if (!this.thread.isDaemon()) {
                this.thread.join(this.getJointime());
            }
            long elapsedTime = System.currentTimeMillis() - beginTime;
            log.info("join thread " + this.getServiceName() + " elapsed time(ms) " + elapsedTime + " "
                    + this.getJointime());
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        }
    }

    public long getJointime() {
        return JOIN_TIME;
    }

    /**
     * 停止该后台服务 【不设置线程打断标识】
     */
    @Deprecated
    public void stop() {
        this.stop(false);
    }

    /**
     * 停止该后台服务 【设置停止位、设置唤醒位、唤醒执行线程、设置线程打断标识】
     */
    @Deprecated
    public void stop(boolean interrupt) {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("stop thread " + this.getServiceName() + " interrupt " + interrupt);

        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }

        if (interrupt) {
            this.thread.interrupt();
        }
    }

    /**
     * 设置后台服务关闭位关闭
     */
    public void makeStop() {
        if (!started.get()) {
            return;
        }
        this.stopped = true;
        log.info("makestop thread " + this.getServiceName());
    }

    /**
     * 唤醒阻塞在CountDownLatch该服务
     */
    public void wakeup() {
        if (hasNotified.compareAndSet(false, true)) {
            waitPoint.countDown(); // notify
        }
    }

    /**
     * 最多阻塞指定间隔等待被唤醒执行、唤醒会回调onWaitEnd扩展点
     */
    protected void waitForRunning(long interval) {
        if (hasNotified.compareAndSet(true, false)) {
            this.onWaitEnd();
            return;
        }

        //entry to wait
        waitPoint.reset();

        try {
            waitPoint.await(interval, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Interrupted", e);
        } finally {
            hasNotified.set(false);
            this.onWaitEnd();
        }
    }

    /**
     * 服务等待被唤醒后回调 【子类扩展用】
     */
    protected void onWaitEnd() {
    }
}
