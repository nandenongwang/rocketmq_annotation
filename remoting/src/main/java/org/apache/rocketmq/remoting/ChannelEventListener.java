package org.apache.rocketmq.remoting;

import io.netty.channel.Channel;

/**
 * channel事件监听器
 * 并处理netty原生channel事件
 */
public interface ChannelEventListener {
    /**
     * 连接
     */
    void onChannelConnect(String remoteAddr, Channel channel);

    /**
     * 关闭
     */
    void onChannelClose(String remoteAddr, Channel channel);

    /**
     * 异常
     */
    void onChannelException(String remoteAddr, Channel channel);

    /**
     * 空闲
     */
    void onChannelIdle(String remoteAddr, Channel channel);
}
