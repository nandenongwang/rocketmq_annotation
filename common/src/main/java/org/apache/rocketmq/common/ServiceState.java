package org.apache.rocketmq.common;

/**
 * 组件启动状态
 */
public enum ServiceState {
    /**
     * 开启启动
     */
    CREATE_JUST,
    /**
     * 运行中 【启动完成后】
     */
    RUNNING,
    /**
     * 关闭
     */
    SHUTDOWN_ALREADY,
    /**
     * 启动失败
     */
    START_FAILED;
}
