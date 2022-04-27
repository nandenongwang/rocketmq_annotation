package org.apache.rocketmq.remoting.netty;

import io.netty.channel.Channel;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 命令执行任务
 */
@EqualsAndHashCode
public class RequestTask implements Runnable {

    /**
     * 处理逻辑
     */
    private final Runnable runnable;

    /**
     * 创建时间
     */
    @Getter
    private final long createTimestamp = System.currentTimeMillis();

    /**
     * 请求客户端channel
     */
    private final Channel channel;

    /**
     * 原始命令
     */
    private final RemotingCommand request;

    /**
     * 是否停止执行
     */
    @Getter
    @Setter
    private boolean stopRun = false;

    public RequestTask(Runnable runnable, Channel channel, RemotingCommand request) {
        this.runnable = runnable;
        this.channel = channel;
        this.request = request;
    }

    @Override
    public void run() {
        if (!this.stopRun) {
            this.runnable.run();
        }
    }

    public void returnResponse(int code, String remark) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(code, remark);
        response.setOpaque(request.getOpaque());
        this.channel.writeAndFlush(response);
    }
}
