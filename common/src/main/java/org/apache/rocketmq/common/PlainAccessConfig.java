package org.apache.rocketmq.common;

import lombok.Data;

import java.util.List;

/**
 * 单个accessKey账户权限配置
 */
@Data
public class PlainAccessConfig {


    /**
     * 账户名
     */
    private String accessKey;

    /**
     * 访问秘钥
     */
    private String secretKey;

    /**
     * 白名单地址
     */
    private String whiteRemoteAddress;

    /**
     * 是否是管理员
     */
    private boolean admin;

    /**
     * 默认topic权限 DENY;PUB;SUB;PUB|SUB
     */
    private String defaultTopicPerm;

    /**
     * 默认消费组权限 DENY;PUB;SUB;PUB|SUB
     */
    private String defaultGroupPerm;

    /**
     * 所有topic权限 【topic=权限】
     */
    private List<String> topicPerms;

    /**
     * 所有消费组权限 【group=权限】
     */
    private List<String> groupPerms;
}
