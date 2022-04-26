package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.protocol.RemotingCommand;

public interface RPCHook {
    /**
     * RPC 前置钩子
     */
    void doBeforeRequest(String remoteAddr, RemotingCommand request);

    /**
     * RPC 后置钩子
     */
    void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response);
}
