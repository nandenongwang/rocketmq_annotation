package org.apache.rocketmq.common;

/**
 * tags过滤类型 【仅使用单tags和sql表达式过滤】
 */
public enum TopicFilterType {

    /**
     * 单tags模式
     */
    SINGLE_TAG,

    /**
     * 多tags模式 【未使用】
     */
    MULTI_TAG

}
