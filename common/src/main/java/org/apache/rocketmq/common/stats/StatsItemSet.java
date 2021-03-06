package org.apache.rocketmq.common.stats;

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 统计指标
 */
public class StatsItemSet {
    private final InternalLogger log;

    /**
     * 统计指标下所有统计单元
     * 统计单元 -> 统计项
     */
    private final ConcurrentMap<String/* topic@consumegroup */, StatsItem> statsItemTable = new ConcurrentHashMap<>(128);

    /**
     * 指标名
     */
    private final String statsName;

    /**
     * 调度线程池
     */
    private final ScheduledExecutorService scheduledExecutorService;

    public StatsItemSet(String statsName, ScheduledExecutorService scheduledExecutorService, InternalLogger log) {
        this.statsName = statsName;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
        this.init();
    }

    /**
     * 初始化定时任务 【采样 & 打印】
     */
    public void init() {
        //10s
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                samplingInSeconds();
            } catch (Throwable ignored) {
            }
        }, 0, 10, TimeUnit.SECONDS);
        //10min
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                samplingInMinutes();
            } catch (Throwable ignored) {
            }
        }, 0, 10, TimeUnit.MINUTES);
        //1h
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                samplingInHour();
            } catch (Throwable ignored) {
            }
        }, 0, 1, TimeUnit.HOURS);

        //1min
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtMinutes();
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60, TimeUnit.MILLISECONDS);
        //1h
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtHour();
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextHourTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60, TimeUnit.MILLISECONDS);
        //1d
        this.scheduledExecutorService.scheduleAtFixedRate(() -> {
            try {
                printAtDay();
            } catch (Throwable ignored) {
            }
        }, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
    }

    //region 对统计指标下所有统计单元进行采样 【秒、分、小时分别采样】
    private void samplingInSeconds() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().samplingInSeconds();
        }
    }

    private void samplingInMinutes() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().samplingInMinutes();
        }
    }

    private void samplingInHour() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().samplingInHour();
        }
    }
    //endregion

    //region 对统计指标下所有统计单元各统计时段的时间段统计快照进行打印 【各topic的消费组(统计单元)分、小时、天时间段统计快照】
    private void printAtMinutes() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().printAtMinutes();
        }
    }

    private void printAtHour() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().printAtHour();
        }
    }

    private void printAtDay() {
        for (Entry<String, StatsItem> next : this.statsItemTable.entrySet()) {
            next.getValue().printAtDay();
        }
    }
    //endregion

    //region 增加统计项统计值 【次数|耗时】
    public void addValue(final String statsKey, final int incValue, final int incTimes) {
        StatsItem statsItem = this.getAndCreateStatsItem(statsKey);
        statsItem.getValue().addAndGet(incValue);
        statsItem.getTimes().addAndGet(incTimes);
    }

    public void addRTValue(final String statsKey, final int incValue, final int incTimes) {
        StatsItem statsItem = this.getAndCreateRTStatsItem(statsKey);
        statsItem.getValue().addAndGet(incValue);
        statsItem.getTimes().addAndGet(incTimes);
    }
    //endregion

    //region 移除统计单元
    public void delValue(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            this.statsItemTable.remove(statsKey);
        }
    }

    public void delValueByPrefixKey(final String statsKey, String separator) {
        this.statsItemTable.entrySet().removeIf(next -> next.getKey().startsWith(statsKey + separator));
    }

    public void delValueByInfixKey(final String statsKey, String separator) {
        this.statsItemTable.entrySet().removeIf(next -> next.getKey().contains(separator + statsKey + separator));
    }

    public void delValueBySuffixKey(final String statsKey, String separator) {
        this.statsItemTable.entrySet().removeIf(next -> next.getKey().endsWith(separator + statsKey));
    }
    //endregion

    //region 获取统计单元统计项 【不存在则创建】
    public StatsItem getAndCreateStatsItem(final String statsKey) {
        return getAndCreateItem(statsKey, false);
    }

    public StatsItem getAndCreateRTStatsItem(final String statsKey) {
        return getAndCreateItem(statsKey, true);
    }

    public StatsItem getAndCreateItem(final String statsKey, boolean rtItem) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null == statsItem) {
            if (rtItem) {
                statsItem = new RTStatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            } else {
                statsItem = new StatsItem(this.statsName, statsKey, this.scheduledExecutorService, this.log);
            }
            StatsItem prev = this.statsItemTable.putIfAbsent(statsKey, statsItem);

            if (null != prev) {
                statsItem = prev;
                // statsItem.init();
            }
        }

        return statsItem;
    }

    public StatsItem getStatsItem(final String statsKey) {
        return this.statsItemTable.get(statsKey);
    }
    //endregion

    /**
     * 获取统计单元统计项分钟段状态快照
     */
    public StatsSnapshot getStatsDataInMinute(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInMinute();
        }
        return new StatsSnapshot();
    }

    /**
     * 获取统计单元统计项小时段状态快照
     */
    public StatsSnapshot getStatsDataInHour(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInHour();
        }
        return new StatsSnapshot();
    }

    /**
     * 获取统计单元统计项一天段状态快照
     */
    public StatsSnapshot getStatsDataInDay(final String statsKey) {
        StatsItem statsItem = this.statsItemTable.get(statsKey);
        if (null != statsItem) {
            return statsItem.getStatsDataInDay();
        }
        return new StatsSnapshot();
    }
}
