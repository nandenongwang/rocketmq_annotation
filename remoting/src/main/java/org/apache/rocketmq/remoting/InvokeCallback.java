package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.netty.ResponseFuture;

public interface InvokeCallback {
    /**
     * 调用回调
     */
    void operationComplete(ResponseFuture responseFuture);
}
