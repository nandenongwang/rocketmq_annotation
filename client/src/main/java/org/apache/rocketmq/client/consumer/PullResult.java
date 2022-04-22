package org.apache.rocketmq.client.consumer;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * 拉取消息结果
 */
@Data
@AllArgsConstructor
public class PullResult {

    /**
     * 拉取状态码
     */
    private final PullStatus pullStatus;
    /**
     * 下次拉取位置
     */
    private final long nextBeginOffset;
    /**
     * 本次拉取消息最小offset
     */
    private final long minOffset;
    /**
     * 本次拉取消息最大offset
     */
    private final long maxOffset;
    /**
     * 本次拉取消息
     */
    private List<MessageExt> msgFoundList;
}
