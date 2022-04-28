package org.apache.rocketmq.remoting.netty;

import lombok.Data;

/**
 * netty服务端配置
 */
@Data
public class NettyServerConfig implements Cloneable {

    /**
     * 监听端口
     */
    private int listenPort = 8888;

    /**
     * 事件处理线程数
     */
    private int serverWorkerThreads = 8;

    /**
     * 异步回调处理线程数
     */
    private int serverCallbackExecutorThreads = 0;

    /**
     * 事件选择线程数
     */
    private int serverSelectorThreads = 3;

    /**
     * 同时处理oneway请求数限制
     */
    private int serverOnewaySemaphoreValue = 256;

    /**
     * 同时异步请求数限制
     */
    private int serverAsyncSemaphoreValue = 64;

    /**
     * 空闲断开超时
     */
    private int serverChannelMaxIdleTimeSeconds = 120;

    /**
     * socket发送缓冲区大小 默认64k
     */
    private int serverSocketSndBufSize = NettySystemConfig.socketSndbufSize;

    /**
     * socket接受缓冲区大小 默认64k
     */
    private int serverSocketRcvBufSize = NettySystemConfig.socketRcvbufSize;

    /**
     * bytebuf缓冲块是否启用池化分配
     */
    private boolean serverPooledByteBufAllocatorEnable = true;

    /**
     * 是否启用epool
     * make make install
     * <p>
     * <p>
     * ../glibc-2.10.1/configure \ --prefix=/usr \ --with-headers=/usr/include \
     * --host=x86_64-linux-gnu \ --build=x86_64-pc-linux-gnu \ --without-gd
     */
    private boolean useEpollNativeSelector = false;

    @Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
