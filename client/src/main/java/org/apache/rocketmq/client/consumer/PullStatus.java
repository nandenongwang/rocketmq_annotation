package org.apache.rocketmq.client.consumer;

/**
 * 拉区消息响应状态
 */
public enum PullStatus {
    /**
     * 拉到新消息
     */
    FOUND,
    /**
     * 没有新消息
     */
    NO_NEW_MSG,
    /**
     * 没有匹配的消息 【有新消息但不符合订阅过滤配置】
     */
    NO_MATCHED_MSG,
    /**
     * 拉取消息开始offset非法 【过大或过小】
     */
    OFFSET_ILLEGAL
}
