package org.apache.rocketmq.client.impl;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 查找broker组通信地址 【默认master】
 */
@AllArgsConstructor
@Data
public class FindBrokerResult {

    /**
     * broker地址
     */
    private final String brokerAddr;

    /**
     * 是否是slave
     */
    private final boolean slave;

    /**
     * broker配置版本
     */
    private final int brokerVersion;

    public FindBrokerResult(String brokerAddr, boolean slave) {
        this.brokerAddr = brokerAddr;
        this.slave = slave;
        this.brokerVersion = 0;
    }
}
