package org.apache.rocketmq.namesrv.routeinfo;

import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.rocketmq.common.DataVersion;

@AllArgsConstructor
@Data
class BrokerLiveInfo {

    /**
     * topic配置最后未变更时间
     */
    private long lastUpdateTimestamp;

    /**
     * topic配置版本号
     */
    private DataVersion dataVersion;

    /**
     * broekr连接
     */
    private Channel channel;

    /**
     * 日志复制监听地址 默认brokerIp:10912
     */
    private String haServerAddr;
}
