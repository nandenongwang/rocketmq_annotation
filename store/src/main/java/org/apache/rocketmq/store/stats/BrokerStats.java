package org.apache.rocketmq.store.stats;

import lombok.Data;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.DefaultMessageStore;

/**
 *
 */
@Data
public class BrokerStats {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final DefaultMessageStore defaultMessageStore;

    public BrokerStats(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    /**
     *
     */
    private volatile long msgPutTotalYesterdayMorning;

    /**
     *
     */
    private volatile long msgPutTotalTodayMorning;

    /**
     *
     */
    private volatile long msgGetTotalYesterdayMorning;

    /**
     *
     */
    private volatile long msgGetTotalTodayMorning;


    public void record() {
        this.msgPutTotalYesterdayMorning = this.msgPutTotalTodayMorning;
        this.msgGetTotalYesterdayMorning = this.msgGetTotalTodayMorning;

        this.msgPutTotalTodayMorning =
                this.defaultMessageStore.getStoreStatsService().getPutMessageTimesTotal();
        this.msgGetTotalTodayMorning =
                this.defaultMessageStore.getStoreStatsService().getGetMessageTransferedMsgCount().get();

        log.info("yesterday put message total: {}", msgPutTotalTodayMorning - msgPutTotalYesterdayMorning);
        log.info("yesterday get message total: {}", msgGetTotalTodayMorning - msgGetTotalYesterdayMorning);
    }
}
