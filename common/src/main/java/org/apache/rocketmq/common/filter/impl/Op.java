package org.apache.rocketmq.common.filter.impl;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public abstract class Op {

    private final String symbol;

}
