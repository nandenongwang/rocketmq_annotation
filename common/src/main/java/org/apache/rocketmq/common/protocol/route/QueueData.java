package org.apache.rocketmq.common.protocol.route;

import lombok.Data;

/**
 * topic队列配置
 */
@Data
public class QueueData implements Comparable<QueueData> {

    /**
     * topic下所有queue所在broker组
     */
    private String brokerName;

    /**
     * readqueue数量
     */
    private int readQueueNums;

    /**
     * writequeue数量
     */
    private int writeQueueNums;

    /**
     * 权限 topic是否可读写等
     * {@link org.apache.rocketmq.common.constant.PermName}
     */
    private int perm;

    /**
     * topic系统flag 无用
     * {@link org.apache.rocketmq.common.sysflag.TopicSysFlag}
     */
    private int topicSynFlag;

    @Override
    public int compareTo(QueueData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

}
