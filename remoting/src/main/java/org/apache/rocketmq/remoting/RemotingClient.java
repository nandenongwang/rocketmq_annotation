package org.apache.rocketmq.remoting;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface RemotingClient extends RemotingService {
    /**
     * 更新nameserver地址
     */
    void updateNameServerAddressList(final List<String> addrs);

    /**
     * 获取nameserver地址
     */
    List<String> getNameServerAddressList();

    /**
     * 同步发送命令
     */
    RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException;

    /**
     * 异步发送命令
     */
    void invokeAsync(String addr, RemotingCommand request, long timeoutMillis, InvokeCallback invokeCallback)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * oneway发送命令
     */
    void invokeOneway(String addr, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingTooMuchRequestException, RemotingTimeoutException, RemotingSendRequestException;

    /**
     * 注册命令处理器
     */
    void registerProcessor(int requestCode, NettyRequestProcessor processor, ExecutorService executor);

    /**
     * 设置回调执行线程池
     */
    void setCallbackExecutor(ExecutorService callbackExecutor);

    /**
     * 获取回调执行线程池
     */
    ExecutorService getCallbackExecutor();

    /**
     * addr对应channel是否可写
     */
    boolean isChannelWritable(String addr);
}
