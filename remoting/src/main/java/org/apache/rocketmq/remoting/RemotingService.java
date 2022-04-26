package org.apache.rocketmq.remoting;

public interface RemotingService {

    /**
     * 启动服务端
     */
    void start();

    /**
     * 关闭服务端
     */
    void shutdown();

    /**
     * 注册RPC hook
     */
    void registerRPCHook(RPCHook rpcHook);
}
