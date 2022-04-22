package org.apache.rocketmq.client.consumer.store;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

/**
 * 存储数据 【queue的消费进度】
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class OffsetSerializeWrapper extends RemotingSerializable {
    private ConcurrentMap<MessageQueue, AtomicLong> offsetTable = new ConcurrentHashMap<>();
}
