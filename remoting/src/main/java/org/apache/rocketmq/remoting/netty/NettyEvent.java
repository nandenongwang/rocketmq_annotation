package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
public class NettyEvent {

    /**
     * 事件类型
     */
    private final NettyEventType type;

    /**
     * 连接地址
     */
    private final String remoteAddr;

    /**
     * 连接channel
     */
    private final Channel channel;
}
