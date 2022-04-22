package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 直接消息消费结果
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class ConsumeMessageDirectlyResult extends RemotingSerializable {
    /**
     * 是否是顺序消息消费结果
     */
    private boolean order = false;

    /**
     * 是否自动提交
     */
    private boolean autoCommit = true;

    /**
     * 消费结果状态
     */
    private CMResult consumeResult;

    /**
     * 异常情况
     */
    private String remark;

    /**
     * 业务侧处理花费时间
     */
    private long spentTimeMills;

}
