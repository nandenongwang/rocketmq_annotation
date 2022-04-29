package org.apache.rocketmq.common.utils;

import io.netty.channel.Channel;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * channel工具
 */
public class ChannelUtil {

    /**
     * 获取channel远程IP地址
     */
    public static String getRemoteIp(Channel channel) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) channel.remoteAddress();
        if (inetSocketAddress == null) {
            return "";
        }
        final InetAddress inetAddr = inetSocketAddress.getAddress();
        return inetAddr != null ? inetAddr.getHostAddress() : inetSocketAddress.getHostName();
    }

}
