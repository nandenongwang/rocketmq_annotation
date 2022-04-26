package org.apache.rocketmq.common.protocol;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.protocol.header.namesrv.RegisterBrokerRequestHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

/**
 * 未使用
 */
@Deprecated
public class MQProtosHelper {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    /**
     * 向nameserver注册broker 【仅注册broker地址】
     */
    public static boolean registerBrokerToNameServer(String nsaddr, String brokerAddr, long timeoutMillis) {
        RegisterBrokerRequestHeader requestHeader = new RegisterBrokerRequestHeader();
        requestHeader.setBrokerAddr(brokerAddr);

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.REGISTER_BROKER, requestHeader);

        try {
            RemotingCommand response = RemotingHelper.invokeSync(nsaddr, request, timeoutMillis);
            return ResponseCode.SUCCESS == response.getCode();
        } catch (Exception e) {
            log.error("Failed to register broker", e);
        }

        return false;
    }
}
