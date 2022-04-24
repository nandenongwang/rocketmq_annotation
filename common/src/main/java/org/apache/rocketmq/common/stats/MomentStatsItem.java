package org.apache.rocketmq.common.stats;

import lombok.Data;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Data
public class MomentStatsItem {
    private final InternalLogger log;


    /**
     * 统计指标名
     */
    private final String statsName;

    /**
     * 统计单元名
     */
    private final String statsKey;

    /**
     * 统计值
     */
    private final AtomicLong value = new AtomicLong(0);

    /**
     * 调度执行线程池
     */
    private final ScheduledExecutorService scheduledExecutorService;

    public MomentStatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, InternalLogger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }

    /**
     * 初始调度任务【周期5min 打印统计值&清空重新计数】
     */
    public void init() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtMinutes();
                MomentStatsItem.this.value.set(0);
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 5, TimeUnit.MILLISECONDS);
    }

    /**
     * 打印统计值
     */
    public void printAtMinutes() {
        log.info(String.format("[%s] [%s] Stats Every 5 Minutes, Value: %d",
                this.statsName,
                this.statsKey,
                this.value.get()));
    }
}
