package org.apache.rocketmq.common.consistenthash;

/**
 * 哈希计算接口
 */
public interface HashFunction {
    long hash(String key);
}
