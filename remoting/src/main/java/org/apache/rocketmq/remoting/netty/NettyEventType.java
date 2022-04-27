package org.apache.rocketmq.remoting.netty;

/**
 * channel管理事件 【连接、关闭、空闲、异常】
 */
public enum NettyEventType {
    CONNECT,
    CLOSE,
    IDLE,
    EXCEPTION
}
