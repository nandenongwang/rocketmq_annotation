package org.apache.rocketmq.common.stats;

import lombok.Getter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MomentStatsItemSet {
    private final InternalLogger log;

    /**
     * 所有统计单元统计状态
     */
    @Getter
    private final ConcurrentMap<String/* key */, MomentStatsItem> statsItemTable = new ConcurrentHashMap<>(128);

    /**
     * 指标名
     */
    @Getter
    private final String statsName;

    /**
     * 统计任务调度线程池
     */
    private final ScheduledExecutorService scheduledExecutorService;

    public MomentStatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, InternalLogger log) {
        this.statsName = statsName;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
        this.init();
    }

    /**
     * 初始化调度任务
     */
    public void init() {
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtMinutes();
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 5, TimeUnit.MILLISECONDS);
    }

    /**
     * 打印所有统计单元分钟段统计数据
     */
    private void printAtMinutes() {
        for (Entry<String, MomentStatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().printAtMinutes();
        }
    }

    /**
     * 增加指定统计单元统计值
     */
    public void setValue(final String statsKey, final int value) {
        MomentStatsItem statsItem = this.getAndCreateStatsItem(statsKey);
        statsItem.getValue().set(value);
    }

    /**
     * 删除统计单元
     */
    public void delValueByInfixKey(String statsKey, String separator) {
        this.statsItemTable.entrySet().removeIf(next -> next.getKey().contains(separator + statsKey + separator));
    }

    /**
     * 删除统计单元
     */
    public void delValueBySuffixKey(String statsKey, String separator) {
        this.statsItemTable.entrySet().removeIf(next -> next.getKey().endsWith(separator + statsKey));
    }

    /**
     * 获取统计单元 【不存在则重新创建】
     */
    public MomentStatsItem getAndCreateStatsItem(String statsKey) {
        MomentStatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {
            statsItem = new MomentStatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            MomentStatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);

            if (null != prev) {
                statsItem = prev;
                // statsItem.init();
            }
        }

        return statsItem;
    }
}
