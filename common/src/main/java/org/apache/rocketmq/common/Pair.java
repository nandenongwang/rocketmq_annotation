package org.apache.rocketmq.common;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 双返回值封装类
 */
@AllArgsConstructor
@Data
public class Pair<T1, T2> {
    private T1 object1;

    private T2 object2;
}
