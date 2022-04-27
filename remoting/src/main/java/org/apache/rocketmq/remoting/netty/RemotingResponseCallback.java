package org.apache.rocketmq.remoting.netty;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 命令响应执行回调 【服务端写出命令请求处理结果后执行】
 */
public interface RemotingResponseCallback {
    void callback(RemotingCommand response);
}
