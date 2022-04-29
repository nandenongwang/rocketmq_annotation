package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * topic配置数据
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class TopicConfigSerializeWrapper extends RemotingSerializable {

    /**
     * 所有topic配置
     */
    private ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();

    /**
     * broker中topic配置版本
     */
    private DataVersion dataVersion = new DataVersion();
}
