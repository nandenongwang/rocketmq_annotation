package org.apache.rocketmq.common.stats;

import lombok.Getter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.LinkedList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class StatsItem {
    private final InternalLogger log;

    /**
     * 统计项递增值
     */
    @Getter
    private final AtomicLong value = new AtomicLong(0);

    /**
     * 统计项递增调用次数
     */
    @Getter
    private final AtomicLong times = new AtomicLong(0);

    /**
     * 统计指标名 如:TOPIC_AND_GROUP_CONSUME_OK_TPS 成功消费tps
     */
    @Getter
    private final String statsName;

    /**
     * 统计单元名称 如:topic@consumergroup
     */
    @Getter
    private final String statsKey;
    /**
     * 每10s采样 限制6条数据
     */
    private final LinkedList<CallSnapshot> csListMinute = new LinkedList<>();

    /**
     * 每10min采样 限制6条数据
     */
    private final LinkedList<CallSnapshot> csListHour = new LinkedList<>();

    /**
     * 每1h采样 限制24条数据
     */
    private final LinkedList<CallSnapshot> csListDay = new LinkedList<>();


    /**
     * 定时调度线程池
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * 定时调度 【采样 & 打印日志】
     */
    public void init() {

        //region 采样 每10秒、10分、1小时采样记录 CallSnapshot，供合成每分、每小时、每天的状态快照 StatsSnapshot
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInSeconds();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.SECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    samplingInHour();
                } catch (Throwable ignored) {
                }
            }
        }, 0, 1, TimeUnit.HOURS);
        //endregion

        //region 打印每分、每小时、每天的状态快照 StatsSnapshot
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtMinutes();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMinutesTimeMillis() - System.currentTimeMillis()), 1000 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtHour();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextHourTimeMillis() - System.currentTimeMillis()), 1000 * 60 * 60, TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    printAtDay();
                } catch (Throwable ignored) {
                }
            }
        }, Math.abs(UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis()) - 2000, 1000 * 60 * 60 * 24, TimeUnit.MILLISECONDS);
        //endregion
    }

    public StatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, InternalLogger log) {
        this.statsName = statsName;
        this.statsKey = statsKey;
        this.scheduledExecutorService = scheduledExecutorService;
        this.log = log;
    }

    /**
     * 根据调用快照集合计算不同时间段的统计快照 【csListMinute|csListHour|csListDay】
     */
    private static StatsSnapshot computeStatsData(final LinkedList<CallSnapshot> csList) {
        StatsSnapshot statsSnapshot = new StatsSnapshot();
        synchronized (csList) {
            double tps = 0;
            double avgpt = 0;
            long sum = 0;
            long timesDiff = 0;
            if (!csList.isEmpty()) {
                CallSnapshot first = csList.getFirst();
                CallSnapshot last = csList.getLast();
                sum = last.getValue() - first.getValue();
                tps = (sum * 1000.0d) / (last.getTimestamp() - first.getTimestamp());

                timesDiff = last.getTimes() - first.getTimes();
                if (timesDiff > 0) {
                    avgpt = (sum * 1.0d) / timesDiff;
                }
            }

            statsSnapshot.setSum(sum);
            statsSnapshot.setTps(tps);
            statsSnapshot.setAvgpt(avgpt);
            statsSnapshot.setTimes(timesDiff);
        }

        return statsSnapshot;
    }

    /**
     * 计算一分钟内统计快照
     */
    public StatsSnapshot getStatsDataInMinute() {
        return computeStatsData(this.csListMinute);
    }

    /**
     * 计算一小时内统计快照
     */
    public StatsSnapshot getStatsDataInHour() {
        return computeStatsData(this.csListHour);
    }

    /**
     * 计算一天内统计快照
     */
    public StatsSnapshot getStatsDataInDay() {
        return computeStatsData(this.csListDay);
    }


    //region 采样 【保存统计项的调用快照】
    public void samplingInSeconds() {
        synchronized (this.csListMinute) {
            if (this.csListMinute.size() == 0) {
                this.csListMinute.add(new CallSnapshot(System.currentTimeMillis() - 10 * 1000, 0, 0));
            }
            this.csListMinute.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value.get()));
            if (this.csListMinute.size() > 7) {
                this.csListMinute.removeFirst();
            }
        }
    }

    public void samplingInMinutes() {
        synchronized (this.csListHour) {
            if (this.csListHour.size() == 0) {
                this.csListHour.add(new CallSnapshot(System.currentTimeMillis() - 10 * 60 * 1000, 0, 0));
            }
            this.csListHour.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value.get()));
            if (this.csListHour.size() > 7) {
                this.csListHour.removeFirst();
            }
        }
    }

    public void samplingInHour() {
        synchronized (this.csListDay) {
            if (this.csListDay.size() == 0) {
                this.csListDay.add(new CallSnapshot(System.currentTimeMillis() - 60 * 60 * 1000, 0, 0));
            }
            this.csListDay.add(new CallSnapshot(System.currentTimeMillis(), this.times.get(), this.value.get()));
            if (this.csListDay.size() > 25) {
                this.csListDay.removeFirst();
            }
        }
    }
    //endregion

    //region 打印 【日志输出各时间段的状态统计】
    public void printAtMinutes() {
        StatsSnapshot ss = computeStatsData(this.csListMinute);
        log.info(String.format("[%s] [%s] Stats In One Minute, ", this.statsName, this.statsKey) + statPrintDetail(ss));
    }

    public void printAtHour() {
        StatsSnapshot ss = computeStatsData(this.csListHour);
        log.info(String.format("[%s] [%s] Stats In One Hour, ", this.statsName, this.statsKey) + statPrintDetail(ss));

    }

    public void printAtDay() {
        StatsSnapshot ss = computeStatsData(this.csListDay);
        log.info(String.format("[%s] [%s] Stats In One Day, ", this.statsName, this.statsKey) + statPrintDetail(ss));
    }

    protected String statPrintDetail(StatsSnapshot ss) {
        return String.format("SUM: %d TPS: %.2f AVGPT: %.2f",
                ss.getSum(),
                ss.getTps(),
                ss.getAvgpt());
    }
    //endregion


}

