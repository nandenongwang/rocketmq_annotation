package org.apache.rocketmq.remoting.netty;

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 命令Controller
 */
public interface NettyRequestProcessor {

    /**
     * 处理远程请求命令
     */
    RemotingCommand processRequest(ChannelHandlerContext ctx, RemotingCommand request) throws Exception;

    boolean rejectRequest();

}
