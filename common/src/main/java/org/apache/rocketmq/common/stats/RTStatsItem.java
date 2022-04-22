package org.apache.rocketmq.common.stats;

import org.apache.rocketmq.logging.InternalLogger;

import java.util.concurrent.ScheduledExecutorService;

/**
 * 统计项值单位为耗时时 avgpt表示平均响应时间
 * A StatItem for response time, the only difference between from StatsItem is it has a different log output.
 */
public class RTStatsItem extends StatsItem {

    public RTStatsItem(String statsName, String statsKey, ScheduledExecutorService scheduledExecutorService, InternalLogger log) {
        super(statsName, statsKey, scheduledExecutorService, log);
    }

    /**
     * For Response Time stat Item, the print detail should be a little different, TPS and SUM makes no sense.
     * And we give a name "AVGRT" rather than AVGPT for value getAvgpt()
     */
    @Override
    protected String statPrintDetail(StatsSnapshot ss) {
        return String.format("TIMES: %d AVGRT: %.2f", ss.getTimes(), ss.getAvgpt());
    }
}
