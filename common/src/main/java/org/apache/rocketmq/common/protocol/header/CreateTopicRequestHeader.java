package org.apache.rocketmq.common.protocol.header;

import lombok.Data;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

/**
 * 创建topic参数
 */
@Data
public class CreateTopicRequestHeader implements CommandCustomHeader {

    /**
     * topic名
     */
    @CFNotNull
    private String topic;

    /**
     * 默认topic 【手动创建topic时无用】
     * 发送未创建topic消息时会根据该默认topic设置新topic配置 【系统默认内置TBW102】
     * 也可手动创建topic后指定为自定义模板topic、设置消息默认topic属性后即可根据新默认topic模版创建topic
     */
    @CFNotNull
    private String defaultTopic;

    /**
     * 读队列数量
     */
    @CFNotNull
    private Integer readQueueNums;

    /**
     * 写队列数量
     */
    @CFNotNull
    private Integer writeQueueNums;

    /**
     * topic权限
     */
    @CFNotNull
    private Integer perm;

    /**
     * 过滤类型
     */
    @CFNotNull
    private String topicFilterType;

    /**
     * 系统flag
     */
    private Integer topicSysFlag;

    /**
     * 是否为顺序消息
     */
    @CFNotNull
    private Boolean order = false;

    @Override
    public void checkFields() throws RemotingCommandException {
        try {
            TopicFilterType.valueOf(this.topicFilterType);
        } catch (Exception e) {
            throw new RemotingCommandException("topicFilterType = [" + topicFilterType + "] value invalid", e);
        }
    }

    public TopicFilterType getTopicFilterTypeEnum() {
        return TopicFilterType.valueOf(this.topicFilterType);
    }
}
