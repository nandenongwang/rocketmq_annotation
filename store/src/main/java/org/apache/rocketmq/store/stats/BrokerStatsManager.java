package org.apache.rocketmq.store.stats;

import java.util.HashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.stats.MomentStatsItemSet;
import org.apache.rocketmq.common.stats.StatsItem;
import org.apache.rocketmq.common.stats.StatsItemSet;

public class BrokerStatsManager {
    /**
     * 日志logger
     */
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.ROCKETMQ_STATS_LOGGER_NAME);
    private static final InternalLogger COMMERCIAL_LOG = InternalLoggerFactory.getLogger(LoggerName.COMMERCIAL_LOGGER_NAME);

    /**
     * 集群名 默认default broker相关指标使用集群名作为统计单元
     */
    private final String clusterName;

    //region 各统计指标名
    //region 存储消息
    /**
     * topic put消息数量 【开启存储rpc消息时包括rpc消息】
     */
    public static final String TOPIC_PUT_NUMS = "TOPIC_PUT_NUMS";

    /**
     * topic put消息总大小
     */
    public static final String TOPIC_PUT_SIZE = "TOPIC_PUT_SIZE";

    /**
     * broker所有topic put消息数量
     */
    public static final String BROKER_PUT_NUMS = "BROKER_PUT_NUMS";
    //endregion

    //region 拉取消息
    /**
     * 消费组拉取指定topic消息总数 【包括事务消息查询次数】
     */
    public static final String GROUP_GET_NUMS = "GROUP_GET_NUMS";

    /**
     * 消费组拉取指定topic消息总大小
     */
    public static final String GROUP_GET_SIZE = "GROUP_GET_SIZE";

    /**
     * broker拉取消息总数
     */
    public static final String BROKER_GET_NUMS = "BROKER_GET_NUMS";
    //endregion

    /**
     * 消费组对指定topic重试消息数
     */
    public static final String SNDBCK_PUT_NUMS = "SNDBCK_PUT_NUMS";

    /**
     * 消费组拉取消息延时
     * Pull Message Latency
     */
    public static final String GROUP_GET_LATENCY = "GROUP_GET_LATENCY";

    /**
     * 消费组落后进度
     */
    public static final String GROUP_GET_FALL_SIZE = "GROUP_GET_FALL_SIZE";

    /**
     * 消费组落后时间
     */
    public static final String GROUP_GET_FALL_TIME = "GROUP_GET_FALL_TIME";
    //endregion

    /**
     * 统计任务调度线程池
     */
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BrokerStatsThread"));

    /**
     * 所有指标统计
     */
    private final HashMap<String, StatsItemSet> statsTable = new HashMap<>();

    /**
     * 消费组落后进度
     */
    @Getter
    private final MomentStatsItemSet momentStatsItemSetFallSize = new MomentStatsItemSet(GROUP_GET_FALL_SIZE, scheduledExecutorService, log);

    /**
     * 消费组落后时间
     */
    @Getter
    private final MomentStatsItemSet momentStatsItemSetFallTime = new MomentStatsItemSet(GROUP_GET_FALL_TIME, scheduledExecutorService, log);

    public BrokerStatsManager(String clusterName) {
        this.clusterName = clusterName;

        this.statsTable.put(TOPIC_PUT_NUMS, new StatsItemSet(TOPIC_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(TOPIC_PUT_SIZE, new StatsItemSet(TOPIC_PUT_SIZE, this.scheduledExecutorService, log));
        this.statsTable.put(GROUP_GET_NUMS, new StatsItemSet(GROUP_GET_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(GROUP_GET_SIZE, new StatsItemSet(GROUP_GET_SIZE, this.scheduledExecutorService, log));
        this.statsTable.put(GROUP_GET_LATENCY, new StatsItemSet(GROUP_GET_LATENCY, this.scheduledExecutorService, log));
        this.statsTable.put(SNDBCK_PUT_NUMS, new StatsItemSet(SNDBCK_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_PUT_NUMS, new StatsItemSet(BROKER_PUT_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_GET_NUMS, new StatsItemSet(BROKER_GET_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(GROUP_GET_FROM_DISK_NUMS, new StatsItemSet(GROUP_GET_FROM_DISK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(GROUP_GET_FROM_DISK_SIZE, new StatsItemSet(GROUP_GET_FROM_DISK_SIZE, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_GET_FROM_DISK_NUMS, new StatsItemSet(BROKER_GET_FROM_DISK_NUMS, this.scheduledExecutorService, log));
        this.statsTable.put(BROKER_GET_FROM_DISK_SIZE, new StatsItemSet(BROKER_GET_FROM_DISK_SIZE, this.scheduledExecutorService, log));

        this.statsTable.put(COMMERCIAL_SEND_TIMES, new StatsItemSet(COMMERCIAL_SEND_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_RCV_TIMES, new StatsItemSet(COMMERCIAL_RCV_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_SEND_SIZE, new StatsItemSet(COMMERCIAL_SEND_SIZE, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_RCV_SIZE, new StatsItemSet(COMMERCIAL_RCV_SIZE, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_RCV_EPOLLS, new StatsItemSet(COMMERCIAL_RCV_EPOLLS, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_SNDBCK_TIMES, new StatsItemSet(COMMERCIAL_SNDBCK_TIMES, this.commercialExecutor, COMMERCIAL_LOG));
        this.statsTable.put(COMMERCIAL_PERM_FAILURES, new StatsItemSet(COMMERCIAL_PERM_FAILURES, this.commercialExecutor, COMMERCIAL_LOG));
    }

    //region 启动&关闭调度线程池
    public void start() {
    }

    public void shutdown() {
        this.scheduledExecutorService.shutdown();
        this.commercialExecutor.shutdown();
    }
    //endregion

    /**
     * 获取指定指标的指定统计单元
     */
    public StatsItem getStatsItem(String statsName, String statsKey) {
        try {
            return this.statsTable.get(statsName).getStatsItem(statsKey);
        } catch (Exception ignored) {
        }

        return null;
    }

    /**
     * 删除topic相关统计单元
     */
    public void onTopicDeleted(final String topic) {
        this.statsTable.get(TOPIC_PUT_NUMS).delValue(topic);
        this.statsTable.get(TOPIC_PUT_SIZE).delValue(topic);
        this.statsTable.get(GROUP_GET_NUMS).delValueByPrefixKey(topic, "@");
        this.statsTable.get(GROUP_GET_SIZE).delValueByPrefixKey(topic, "@");
        this.statsTable.get(SNDBCK_PUT_NUMS).delValueByPrefixKey(topic, "@");
        this.statsTable.get(GROUP_GET_LATENCY).delValueByInfixKey(topic, "@");
        this.momentStatsItemSetFallSize.delValueByInfixKey(topic, "@");
        this.momentStatsItemSetFallTime.delValueByInfixKey(topic, "@");
    }

    /**
     * 删除消费组相关统计单元
     */
    public void onGroupDeleted(final String group) {
        this.statsTable.get(GROUP_GET_NUMS).delValueBySuffixKey(group, "@");
        this.statsTable.get(GROUP_GET_SIZE).delValueBySuffixKey(group, "@");
        this.statsTable.get(SNDBCK_PUT_NUMS).delValueBySuffixKey(group, "@");
        this.statsTable.get(GROUP_GET_LATENCY).delValueBySuffixKey(group, "@");
        this.momentStatsItemSetFallSize.delValueBySuffixKey(group, "@");
        this.momentStatsItemSetFallTime.delValueBySuffixKey(group, "@");
    }

    //region 增加指标值

    /**
     * 测试使用
     */
    public void incTopicPutNums(String topic) {
        this.statsTable.get(TOPIC_PUT_NUMS).addValue(topic, 1, 1);
    }

    /**
     * 增加topic put消息数
     */
    public void incTopicPutNums(String topic, int num, int times) {
        this.statsTable.get(TOPIC_PUT_NUMS).addValue(topic, num, times);
    }

    /**
     * 增加topic put消息总大小
     */
    public void incTopicPutSize(String topic, int size) {
        this.statsTable.get(TOPIC_PUT_SIZE).addValue(topic, size, 1);
    }

    /**
     * 测试使用
     */
    public void incBrokerPutNums() {
        this.statsTable.get(BROKER_PUT_NUMS).getAndCreateStatsItem(this.clusterName).getValue().incrementAndGet();
    }

    /**
     * 增加broker put消息总数(包括所有topic)
     */
    public void incBrokerPutNums(int incValue) {
        this.statsTable.get(BROKER_PUT_NUMS).getAndCreateStatsItem(this.clusterName).getValue().addAndGet(incValue);
    }

    /**
     * 消费组拉取指定topic消息总数
     */
    public void incGroupGetNums(String group, String topic, int incValue) {
        final String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(GROUP_GET_NUMS).addValue(statsKey, incValue, 1);
    }

    /**
     * 消费组拉取指定topic的tps
     */
    public double tpsGroupGetNums(String group, String topic) {
        String statsKey = buildStatsKey(topic, group);
        return this.statsTable.get(GROUP_GET_NUMS).getStatsDataInMinute(statsKey).getTps();
    }

    /**
     * 消费组拉取指定topic消息总大小
     */
    public void incGroupGetSize(String group, String topic, int incValue) {
        String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(GROUP_GET_SIZE).addValue(statsKey, incValue, 1);
    }

    /**
     * broker拉取消息总数
     */
    public void incBrokerGetNums(int incValue) {
        this.statsTable.get(BROKER_GET_NUMS).getAndCreateStatsItem(this.clusterName).getValue().addAndGet(incValue);
    }

    /**
     * 消费组对指定topic重试消息数
     */
    public void incSendBackNums(String group, String topic) {
        String statsKey = buildStatsKey(topic, group);
        this.statsTable.get(SNDBCK_PUT_NUMS).addValue(statsKey, 1, 1);
    }


    /**
     * 消费组落后时间
     */
    public void recordDiskFallBehindTime(String group, String topic, int queueId, long fallBehind) {
        String statsKey = String.format("%d@%s@%s", queueId, topic, group);
        this.momentStatsItemSetFallTime.getAndCreateStatsItem(statsKey).getValue().set(fallBehind);
    }

    /**
     * 消费组落后进度
     */
    public void recordDiskFallBehindSize(String group, String topic, int queueId, long fallBehind) {
        String statsKey = String.format("%d@%s@%s", queueId, topic, group);
        this.momentStatsItemSetFallSize.getAndCreateStatsItem(statsKey).getValue().set(fallBehind);
    }

    //endregion

    public String buildStatsKey(String topic, String group) {
        return topic + "@" + group;
    }

    /**
     * 消费组读取消息延时
     */
    public void incGroupGetLatency(String group, String topic, int queueId, int incValue) {
        final String statsKey = String.format("%d@%s@%s", queueId, topic, group);
        this.statsTable.get(GROUP_GET_LATENCY).addValue(statsKey, incValue, 1);
    }


    //region 未使用

    // Message Size limit for one api-calling count.
    public static final double SIZE_PER_COUNT = 64 * 1024;

    public static final String GROUP_GET_FROM_DISK_NUMS = "GROUP_GET_FROM_DISK_NUMS";
    public static final String GROUP_GET_FROM_DISK_SIZE = "GROUP_GET_FROM_DISK_SIZE";
    public static final String BROKER_GET_FROM_DISK_NUMS = "BROKER_GET_FROM_DISK_NUMS";
    public static final String BROKER_GET_FROM_DISK_SIZE = "BROKER_GET_FROM_DISK_SIZE";
    // For commercial
    public static final String COMMERCIAL_SEND_TIMES = "COMMERCIAL_SEND_TIMES";
    public static final String COMMERCIAL_SNDBCK_TIMES = "COMMERCIAL_SNDBCK_TIMES";
    public static final String COMMERCIAL_RCV_TIMES = "COMMERCIAL_RCV_TIMES";
    public static final String COMMERCIAL_RCV_EPOLLS = "COMMERCIAL_RCV_EPOLLS";
    public static final String COMMERCIAL_SEND_SIZE = "COMMERCIAL_SEND_SIZE";
    public static final String COMMERCIAL_RCV_SIZE = "COMMERCIAL_RCV_SIZE";
    public static final String COMMERCIAL_PERM_FAILURES = "COMMERCIAL_PERM_FAILURES";
    public static final String COMMERCIAL_OWNER = "Owner";
    private final ScheduledExecutorService commercialExecutor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("CommercialStatsThread"));

    public void incCommercialValue(String key, String owner, String group, String topic, String type, int incValue) {
        String statsKey = buildCommercialStatsKey(owner, topic, group, type);
        this.statsTable.get(key).addValue(statsKey, incValue, 1);
    }

    public String buildCommercialStatsKey(String owner, String topic, String group, String type) {
        return owner + "@" + topic + "@" + group + "@" + type;
    }

    public enum StatsType {
        SEND_SUCCESS,
        SEND_FAILURE,
        SEND_BACK,
        SEND_TIMER,
        SEND_TRANSACTION,
        RCV_SUCCESS,
        RCV_EPOLLS,
        PERM_FAILURE
    }
    //endregion
}
