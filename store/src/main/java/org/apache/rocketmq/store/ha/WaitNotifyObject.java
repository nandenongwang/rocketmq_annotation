package org.apache.rocketmq.store.ha;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * 等待唤醒 线程调度工具
 */
public class WaitNotifyObject {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    protected final HashMap<Long/* thread id */, Boolean/* notified */> waitingThreadTable = new HashMap<>(16);

    protected volatile boolean hasNotified = false;

    public void wakeup() {
        synchronized (this) {
            if (!this.hasNotified) {
                this.hasNotified = true;
                this.notify();
            }
        }
    }

    protected void waitForRunning(long interval) {
        synchronized (this) {
            if (this.hasNotified) {
                this.hasNotified = false;
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.hasNotified = false;
                this.onWaitEnd();
            }
        }
    }

    protected void onWaitEnd() {
    }

    public void wakeupAll() {
        synchronized (this) {
            boolean needNotify = false;

            for (Map.Entry<Long, Boolean> entry : this.waitingThreadTable.entrySet()) {
                needNotify = needNotify || !entry.getValue();
                entry.setValue(true);
            }

            if (needNotify) {
                this.notifyAll();
            }
        }
    }

    public void allWaitForRunning(long interval) {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            Boolean notified = this.waitingThreadTable.get(currentThreadId);
            if (notified != null && notified) {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
                return;
            }

            try {
                this.wait(interval);
            } catch (InterruptedException e) {
                log.error("Interrupted", e);
            } finally {
                this.waitingThreadTable.put(currentThreadId, false);
                this.onWaitEnd();
            }
        }
    }

    public void removeFromWaitingThreadTable() {
        long currentThreadId = Thread.currentThread().getId();
        synchronized (this) {
            this.waitingThreadTable.remove(currentThreadId);
        }
    }
}
