package org.apache.rocketmq.store.config;

/**
 * broker角色
 */
public enum BrokerRole {
    /**
     * 异步刷盘master
     */
    ASYNC_MASTER,

    /**
     * 同步刷盘master
     */
    SYNC_MASTER,

    /**
     * slave
     */
    SLAVE;
}
