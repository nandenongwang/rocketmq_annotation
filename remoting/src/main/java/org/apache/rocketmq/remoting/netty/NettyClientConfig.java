package org.apache.rocketmq.remoting.netty;

import lombok.Data;

/**
 * netty客户端配置
 */
@Data
public class NettyClientConfig {

    /**
     * Worker thread number
     */
    private int clientWorkerThreads = 4;

    /**
     * 客户端公用命令处理线程数
     */
    private int clientCallbackExecutorThreads = Runtime.getRuntime().availableProcessors();

    /**
     * 同时发送oneway请求数限制
     */
    private int clientOnewaySemaphoreValue = NettySystemConfig.CLIENT_ONEWAY_SEMAPHORE_VALUE;

    /**
     * 同时发送异步请求数限制
     */
    private int clientAsyncSemaphoreValue = NettySystemConfig.CLIENT_ASYNC_SEMAPHORE_VALUE;

    /**
     * socket连接超时
     */
    private int connectTimeoutMillis = 3000;

    /**
     * 未使用
     */
    private long channelNotActiveInterval = 1000 * 60;

    /**
     * 空闲检测超时
     * IdleStateEvent will be triggered when neither read nor write was performed for
     * the specified period of this time. Specify {@code 0} to disable
     */
    private int clientChannelMaxIdleTimeSeconds = 120;

    /**
     * socket发送缓冲区大小
     */
    private int clientSocketSndBufSize = NettySystemConfig.socketSndbufSize;

    /**
     * socket接收缓冲区大小
     */
    private int clientSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

    /**
     * 是否开启bytebuf池化分配
     */
    private boolean clientPooledByteBufAllocatorEnable = false;

    /**
     * 请求执行超时是否关闭channel
     */
    private boolean clientCloseSocketIfTimeout = false;

    /**
     * 是否启用tls
     */
    private boolean useTLS;
}
