package org.apache.rocketmq.store.index;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class QueryOffsetResult {

    /**
     * 满足查询调节的commitlogoffset
     */
    private final List<Long> phyOffsets;

    /**
     * 索引最后更新时间
     */
    private final long indexLastUpdateTimestamp;

    /**
     * 索引最大commitlogoffset
     */
    private final long indexLastUpdatePhyoffset;
}
