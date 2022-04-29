package org.apache.rocketmq.common;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.common.constant.PermName;

/**
 * topic配置
 */
@Data
@NoArgsConstructor
public class TopicConfig {

    /**
     *topic配置分隔符 【注册broker时topic配置内容】
     */
    private static final String SEPARATOR = " ";

    /**
     *默认读queue数量
     */
    public static int defaultReadQueueNums = 16;

    /**
     *默认写queue数量
     */
    public static int defaultWriteQueueNums = 16;

    /**
     *topic名
     */
    private String topicName;

    /**
     * 读queue数量 默认16
     */
    private int readQueueNums = defaultReadQueueNums;

    /**
     * 写queue数量 默认16
     */
    private int writeQueueNums = defaultWriteQueueNums;

    /**
     * topic读写权限
     */
    private int perm = PermName.PERM_READ | PermName.PERM_WRITE;

    /**
     *过滤类型
     */
    private TopicFilterType topicFilterType = TopicFilterType.SINGLE_TAG;

    /**
     *topic系统flag
     */
    private int topicSysFlag = 0;

    /**
     * 是否是顺序topic
     */
    private boolean order = false;

    public TopicConfig(String topicName) {
        this.topicName = topicName;
    }

    public TopicConfig(String topicName, int readQueueNums, int writeQueueNums, int perm) {
        this.topicName = topicName;
        this.readQueueNums = readQueueNums;
        this.writeQueueNums = writeQueueNums;
        this.perm = perm;
    }

    /**
     * 编码该topic配置 【注册broker时消息体中topic配置】
     */
    public String encode() {
        return this.topicName +
                SEPARATOR +
                this.readQueueNums +
                SEPARATOR +
                this.writeQueueNums +
                SEPARATOR +
                this.perm +
                SEPARATOR +
                this.topicFilterType;
    }

    /**
     * 解码该topic配置 【注册broker时消息体中topic配置】
     */
    public boolean decode(String in) {
        String[] strs = in.split(SEPARATOR);
        if (strs.length == 5) {
            this.topicName = strs[0];
            this.readQueueNums = Integer.parseInt(strs[1]);
            this.writeQueueNums = Integer.parseInt(strs[2]);
            this.perm = Integer.parseInt(strs[3]);
            this.topicFilterType = TopicFilterType.valueOf(strs[4]);
            return true;
        }
        return false;
    }
}
