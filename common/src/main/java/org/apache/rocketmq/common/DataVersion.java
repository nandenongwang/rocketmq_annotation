package org.apache.rocketmq.common;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.concurrent.atomic.AtomicLong;

/**
 * topic配置版本
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class DataVersion extends RemotingSerializable {

    /**
     *
     */
    private long timestamp = System.currentTimeMillis();

    /**
     *
     */
    private AtomicLong counter = new AtomicLong(0);

    public void assignNewOne(final DataVersion dataVersion) {
        this.timestamp = dataVersion.timestamp;
        this.counter.set(dataVersion.counter.get());
    }

    public void nextVersion() {
        this.timestamp = System.currentTimeMillis();
        this.counter.incrementAndGet();
    }
}
