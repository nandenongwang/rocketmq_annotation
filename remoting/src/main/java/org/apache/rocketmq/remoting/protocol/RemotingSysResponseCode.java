package org.apache.rocketmq.remoting.protocol;

/**
 * 系统响应码
 */
public class RemotingSysResponseCode {

    /**
     * 成功
     */
    public static final int SUCCESS = 0;

    /**
     * 系统错误 如：crc校验失败等
     */
    public static final int SYSTEM_ERROR = 1;

    /**
     * 命令处理队列积压命令过多
     */
    public static final int SYSTEM_BUSY = 2;

    /**
     * 未找到该命令码处理程序
     */
    public static final int REQUEST_CODE_NOT_SUPPORTED = 3;

    /**
     * 事务失败
     */
    public static final int TRANSACTION_FAILED = 4;
}
