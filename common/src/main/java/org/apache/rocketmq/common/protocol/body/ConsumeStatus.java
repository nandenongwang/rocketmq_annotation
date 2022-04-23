package org.apache.rocketmq.common.protocol.body;

import lombok.Data;

/**
 *
 */
@Data
public class ConsumeStatus {

    /**
     *
     */
    private double pullRT;

    /**
     *
     */
    private double pullTPS;

    /**
     *
     */
    private double consumeRT;

    /**
     *
     */
    private double consumeOKTPS;

    /**
     *
     */
    private double consumeFailedTPS;

    /**
     *
     */
    private long consumeFailedMsgs;
}
