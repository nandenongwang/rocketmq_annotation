package org.apache.rocketmq.common.stats;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 统计项调用快照 【记录统计时瞬时值】
 */
@Data
@AllArgsConstructor
class CallSnapshot {

    /**
     * 记录快照时间戳
     */
    private long timestamp;

    /**
     * 记录快照时累计调用次数
     */
    private long times;

    /**
     * 记录快照时累计统计项值
     */
    private long value;
}
