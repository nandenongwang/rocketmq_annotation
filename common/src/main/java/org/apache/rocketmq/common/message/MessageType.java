package org.apache.rocketmq.common.message;

/**
 * 发送消息类型
 */
public enum MessageType {
    /**
     * 普通消息(包括重试)
     */
    Normal_Msg,

    /**
     * 事务消息(发送的事务消息为half消息)
     */
    Trans_Msg_Half,

    /**
     * 事务消息(提交类型 无使用)
     */
    Trans_msg_Commit,

    /**
     * 延时消息
     */
    Delay_Msg,
}
