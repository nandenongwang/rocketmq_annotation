package org.apache.rocketmq.store.config;

import java.io.File;

public class StorePathConfigHelper {

    /**
     * 消费队列文件
     */
    public static String getStorePathConsumeQueue(final String rootDir) {
        return rootDir + File.separator + "consumequeue";
    }

    /**
     * 消息扩展段存储文件
     */
    public static String getStorePathConsumeQueueExt(final String rootDir) {
        return rootDir + File.separator + "consumequeue_ext";
    }

    /**
     * 索引文件
     */
    public static String getStorePathIndex(final String rootDir) {
        return rootDir + File.separator + "index";
    }

    /**
     * 保存点文件
     */
    public static String getStoreCheckpoint(final String rootDir) {
        return rootDir + File.separator + "checkpoint";
    }

    /**
     * 异常关闭文件
     */
    public static String getAbortFile(final String rootDir) {
        return rootDir + File.separator + "abort";
    }

    /**
     * 启动锁文件
     */
    public static String getLockFile(final String rootDir) {
        return rootDir + File.separator + "lock";
    }

    /**
     * 延时消息进度文件
     */
    public static String getDelayOffsetStorePath(final String rootDir) {
        return rootDir + File.separator + "config" + File.separator + "delayOffset.json";
    }

}
