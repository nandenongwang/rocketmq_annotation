package org.apache.rocketmq.remoting.netty;

/**
 * netty系统属性配置
 */
public class NettySystemConfig {

    /**
     * 是否启用池化bytebuf分配
     */
    public static final String COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE = "com.rocketmq.remoting.nettyPooledByteBufAllocatorEnable";
    public static final boolean NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE = Boolean.parseBoolean(System.getProperty(COM_ROCKETMQ_REMOTING_NETTY_POOLED_BYTE_BUF_ALLOCATOR_ENABLE, "false"));

    /**
     * 同时异步请求数限制
     */
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE = "com.rocketmq.remoting.clientAsyncSemaphoreValue";
    public static final int CLIENT_ASYNC_SEMAPHORE_VALUE = Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ASYNC_SEMAPHORE_VALUE, "65535"));

    /**
     * 同时oneway请求数限制
     */
    public static final String COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE = "com.rocketmq.remoting.clientOnewaySemaphoreValue";
    public static final int CLIENT_ONEWAY_SEMAPHORE_VALUE = Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_CLIENT_ONEWAY_SEMAPHORE_VALUE, "65535"));

    /**
     * 发送缓冲区大小
     */
    public static final String COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE = "com.rocketmq.remoting.socket.sndbuf.size";
    public static int socketSndbufSize = Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_SNDBUF_SIZE, "65535"));

    /**
     * 接收缓冲区大小
     */
    public static final String COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE = "com.rocketmq.remoting.socket.rcvbuf.size";
    public static int socketRcvbufSize = Integer.parseInt(System.getProperty(COM_ROCKETMQ_REMOTING_SOCKET_RCVBUF_SIZE, "65535"));
}
