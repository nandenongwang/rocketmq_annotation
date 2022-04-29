package org.apache.rocketmq.common.subscription;

import lombok.Data;
import org.apache.rocketmq.common.MixAll;

/**
 * 消费组订阅配置
 */
@Data
public class SubscriptionGroupConfig {

    /**
     * 消费组名
     */
    private String groupName;

    /**
     *
     */
    private boolean consumeEnable = true;

    /**
     *
     */
    private boolean consumeFromMinEnable = true;

    /**
     *
     */
    private boolean consumeBroadcastEnable = true;

    /**
     *
     */
    private int retryQueueNums = 1;

    /**
     *
     */
    private int retryMaxTimes = 16;

    /**
     *
     */
    private long brokerId = MixAll.MASTER_ID;

    /**
     *
     */
    private long whichBrokerWhenConsumeSlowly = 1;

    /**
     *
     */
    private boolean notifyConsumerIdsChangedEnable = true;


    public boolean isConsumeEnable() {
        return consumeEnable;
    }

    public boolean isConsumeFromMinEnable() {
        return consumeFromMinEnable;
    }

    public boolean isConsumeBroadcastEnable() {
        return consumeBroadcastEnable;
    }

    public boolean isNotifyConsumerIdsChangedEnable() {
        return notifyConsumerIdsChangedEnable;
    }
}
