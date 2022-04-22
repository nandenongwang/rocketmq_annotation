package org.apache.rocketmq.client.stat;

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.protocol.body.ConsumeStatus;
import org.apache.rocketmq.common.stats.StatsItemSet;
import org.apache.rocketmq.common.stats.StatsSnapshot;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.concurrent.ScheduledExecutorService;

/**
 * 消费者统计管理器
 */
public class ConsumerStatsManager {
    private static final InternalLogger log = ClientLogger.getLog();

    //region 各统计指标
    /**
     * 消息成功消费tps
     */
    private static final String TOPIC_AND_GROUP_CONSUME_OK_TPS = "CONSUME_OK_TPS";
    private final StatsItemSet topicAndGroupConsumeOKTPS;

    /**
     * 消息失败消费tps
     */
    private static final String TOPIC_AND_GROUP_CONSUME_FAILED_TPS = "CONSUME_FAILED_TPS";
    private final StatsItemSet topicAndGroupConsumeFailedTPS;

    /**
     * 消息消费平均耗时
     */
    private static final String TOPIC_AND_GROUP_CONSUME_RT = "CONSUME_RT";
    private final StatsItemSet topicAndGroupConsumeRT;

    /**
     * 消息拉取tps
     */
    private static final String TOPIC_AND_GROUP_PULL_TPS = "PULL_TPS";
    private final StatsItemSet topicAndGroupPullTPS;

    /**
     * 消息拉取平均耗时
     */
    private static final String TOPIC_AND_GROUP_PULL_RT = "PULL_RT";
    private final StatsItemSet topicAndGroupPullRT;
    //endregion

    public ConsumerStatsManager(final ScheduledExecutorService scheduledExecutorService) {
        this.topicAndGroupConsumeOKTPS = new StatsItemSet(TOPIC_AND_GROUP_CONSUME_OK_TPS, scheduledExecutorService, log);

        this.topicAndGroupConsumeRT = new StatsItemSet(TOPIC_AND_GROUP_CONSUME_RT, scheduledExecutorService, log);

        this.topicAndGroupConsumeFailedTPS = new StatsItemSet(TOPIC_AND_GROUP_CONSUME_FAILED_TPS, scheduledExecutorService, log);

        this.topicAndGroupPullTPS = new StatsItemSet(TOPIC_AND_GROUP_PULL_TPS, scheduledExecutorService, log);

        this.topicAndGroupPullRT = new StatsItemSet(TOPIC_AND_GROUP_PULL_RT, scheduledExecutorService, log);
    }

    //region 增加各类型采样值
    public void incPullRT(final String group, final String topic, final long rt) {
        this.topicAndGroupPullRT.addRTValue(topic + "@" + group, (int) rt, 1);
    }

    public void incPullTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupPullTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public void incConsumeRT(final String group, final String topic, final long rt) {
        this.topicAndGroupConsumeRT.addRTValue(topic + "@" + group, (int) rt, 1);
    }

    public void incConsumeOKTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeOKTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }

    public void incConsumeFailedTPS(final String group, final String topic, final long msgs) {
        this.topicAndGroupConsumeFailedTPS.addValue(topic + "@" + group, (int) msgs, 1);
    }
    //endregion

    /**
     * 聚合各指标分钟段统计快照得到消费者统计状态
     */
    public ConsumeStatus consumeStatus(final String group, final String topic) {
        ConsumeStatus cs = new ConsumeStatus();
        {
            StatsSnapshot ss = this.getPullRT(group, topic);
            if (ss != null) {
                cs.setPullRT(ss.getAvgpt());
            }
        }

        {
            StatsSnapshot ss = this.getPullTPS(group, topic);
            if (ss != null) {
                cs.setPullTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeRT(group, topic);
            if (ss != null) {
                cs.setConsumeRT(ss.getAvgpt());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeOKTPS(group, topic);
            if (ss != null) {
                cs.setConsumeOKTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.getConsumeFailedTPS(group, topic);
            if (ss != null) {
                cs.setConsumeFailedTPS(ss.getTps());
            }
        }

        {
            StatsSnapshot ss = this.topicAndGroupConsumeFailedTPS.getStatsDataInHour(topic + "@" + group);
            if (ss != null) {
                cs.setConsumeFailedMsgs(ss.getSum());
            }
        }

        return cs;
    }

    //region 获取各类型采样快照

    /**
     * 获取【拉取消息平均响应时间】指定topic@group统计单元的分钟段快照
     */
    private StatsSnapshot getPullRT(final String group, final String topic) {
        return this.topicAndGroupPullRT.getStatsDataInMinute(topic + "@" + group);
    }

    /**
     * 获取【拉取消息tps】指定topic@group统计单元的分钟段快照
     */
    private StatsSnapshot getPullTPS(final String group, final String topic) {
        return this.topicAndGroupPullTPS.getStatsDataInMinute(topic + "@" + group);
    }

    /**
     * 获取【消费消息平均响应时间】指定topic@group统计单元的分钟段快照 【最近一分钟消费消息数为0时获取小时段】
     */
    private StatsSnapshot getConsumeRT(final String group, final String topic) {
        StatsSnapshot statsData = this.topicAndGroupConsumeRT.getStatsDataInMinute(topic + "@" + group);
        if (0 == statsData.getSum()) {
            statsData = this.topicAndGroupConsumeRT.getStatsDataInHour(topic + "@" + group);
        }

        return statsData;
    }

    /**
     * 获取【消息消费成功tps】指定topic@group统计单元的分钟段快照
     */
    private StatsSnapshot getConsumeOKTPS(final String group, final String topic) {
        return this.topicAndGroupConsumeOKTPS.getStatsDataInMinute(topic + "@" + group);
    }

    /**
     * 获取【消息消费失败tps】指定topic@group统计单元的分钟段快照
     */
    private StatsSnapshot getConsumeFailedTPS(final String group, final String topic) {
        return this.topicAndGroupConsumeFailedTPS.getStatsDataInMinute(topic + "@" + group);
    }
    //endregion

    public void start() {
    }

    public void shutdown() {
    }

}
