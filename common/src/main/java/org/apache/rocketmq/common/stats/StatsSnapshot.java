package org.apache.rocketmq.common.stats;

import lombok.Data;

/**
 * 时间段统计快照 【保存计算后的时间段各统计值】
 */
@Data
public class StatsSnapshot {
    /**
     * 时间段内统计项值的总增量
     */
    private long sum;

    /**
     * 时间段内统计项值的tps
     */
    private double tps;

    /**
     * 时间段内统计项调用次数
     */
    private long times;

    /**
     * 时间段内统计项值每次调用平均值的增量【sum/times】
     * 如果统计值为耗时、则该项为 总耗时/总调用次数 = 平均响应时间
     */
    private double avgpt;
}
