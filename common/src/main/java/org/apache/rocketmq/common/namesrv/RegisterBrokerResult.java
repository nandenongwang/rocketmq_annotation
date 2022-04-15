package org.apache.rocketmq.common.namesrv;

import lombok.Data;
import org.apache.rocketmq.common.protocol.body.KVTable;

@Data
public class RegisterBrokerResult {
    /**
     * slave地址
     */
    private String haServerAddr;
    /**
     * master地址
     */
    private String masterAddr;
    /**
     * nameserver配置表
     */
    private KVTable kvTable;
}
