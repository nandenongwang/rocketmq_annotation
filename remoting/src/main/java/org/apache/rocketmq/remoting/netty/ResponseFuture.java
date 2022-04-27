package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import lombok.Data;
import org.apache.rocketmq.remoting.InvokeCallback;
import org.apache.rocketmq.remoting.common.SemaphoreReleaseOnlyOnce;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 响应future
 */
@Data
public class ResponseFuture {

    /**
     * 请求ID
     */
    private final int opaque;

    /**
     * 写入channel
     */
    private final Channel processChannel;

    /**
     * 剩余超时时间
     */
    private final long timeoutMillis;

    /**
     * 异步调用回调
     */
    private final InvokeCallback invokeCallback;

    /**
     * 挂起时间
     */
    private final long beginTimestamp = System.currentTimeMillis();

    /**
     * 同步等待CountDownLatch 【请求wait、响应命令countdown】
     */
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    /**
     * 流控信号量
     */
    private final SemaphoreReleaseOnlyOnce once;

    /**
     * 异步调用回调是否已经执行
     */
    private final AtomicBoolean executeCallbackOnlyOnce = new AtomicBoolean(false);

    /**
     * 响应结果
     */
    private volatile RemotingCommand responseCommand;

    /**
     * 是否发送成功
     */
    private volatile boolean sendRequestOK = true;

    /**
     * 发送失败原因
     */
    private volatile Throwable cause;

    public ResponseFuture(Channel channel, int opaque, long timeoutMillis, InvokeCallback invokeCallback, SemaphoreReleaseOnlyOnce once) {
        this.opaque = opaque;
        this.processChannel = channel;
        this.timeoutMillis = timeoutMillis;
        this.invokeCallback = invokeCallback;
        this.once = once;
    }

    /**
     * 执行异步调用回调
     */
    public void executeInvokeCallback() {
        if (invokeCallback != null) {
            if (this.executeCallbackOnlyOnce.compareAndSet(false, true)) {
                invokeCallback.operationComplete(this);
            }
        }
    }

    /**
     * 释放流控信号量
     */
    public void release() {
        if (this.once != null) {
            this.once.release();
        }
    }

    /**
     * 请求是否超时
     */
    public boolean isTimeout() {
        long diff = System.currentTimeMillis() - this.beginTimestamp;
        return diff > this.timeoutMillis;
    }

    /**
     * 阻塞等待响应
     */
    public RemotingCommand waitResponse(final long timeoutMillis) throws InterruptedException {
        this.countDownLatch.await(timeoutMillis, TimeUnit.MILLISECONDS);
        return this.responseCommand;
    }

    /**
     * 设置响应结果并唤醒阻塞线程
     */
    public void putResponse(final RemotingCommand responseCommand) {
        this.responseCommand = responseCommand;
        this.countDownLatch.countDown();
    }
}
