package org.apache.rocketmq.client.impl;

/**
 * 通信模式
 */
public enum CommunicationMode {
    /**
     * 同步 需阻塞等待响应
     */
    SYNC,
    /**
     *异步 通过回调处理响应
     */
    ASYNC,
    /**
     *oneway 写出请求立即返回
     */
    ONEWAY,
}
