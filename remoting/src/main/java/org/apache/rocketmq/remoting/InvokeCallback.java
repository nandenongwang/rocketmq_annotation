package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.netty.ResponseFuture;

/**
 * 异步发送命令回调
 */
public interface InvokeCallback {

    /**
     * 调用回调
     */
    void operationComplete(ResponseFuture responseFuture);
}
