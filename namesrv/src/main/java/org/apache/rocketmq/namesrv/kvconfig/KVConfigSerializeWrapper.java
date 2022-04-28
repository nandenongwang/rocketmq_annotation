package org.apache.rocketmq.namesrv.kvconfig;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.HashMap;

/**
 * 配置存储数据
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class KVConfigSerializeWrapper extends RemotingSerializable {
    private HashMap<String/* Namespace */, HashMap<String/* Key */, String/* Value */>> configTable;
}
