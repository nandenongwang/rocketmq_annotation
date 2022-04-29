package org.apache.rocketmq.common;

import lombok.Data;

import java.util.List;

/**
 * 全局权限配置
 */
@Data
public class AclConfig {

    /**
     * 全局白名单
     */
    private List<String> globalWhiteAddrs;

    /**
     * 所有账户权限配置
     */
    private List<PlainAccessConfig> plainAccessConfigs;

}
