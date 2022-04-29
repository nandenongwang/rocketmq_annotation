package org.apache.rocketmq.common.namesrv;

import lombok.Data;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;

/**
 * nameserver配置
 */
@Data
public class NamesrvConfig {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.NAMESRV_LOGGER_NAME);

    /**
     * 家目录
     */
    private String rocketmqHome = System.getProperty(MixAll.ROCKETMQ_HOME_PROPERTY, System.getenv(MixAll.ROCKETMQ_HOME_ENV));

    /**
     * KV配置存储目录
     */
    private String kvConfigPath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "kvConfig.json";

    /**
     * nameserver配置文件目录
     */
    private String configStorePath = System.getProperty("user.home") + File.separator + "namesrv" + File.separator + "namesrv.properties";

    /**
     * 生产环境名
     */
    private String productEnvName = "center";

    /**
     * clusterTest
     */
    private boolean clusterTest = false;

    /**
     * 是否开启存储顺序消息配置
     */
    private boolean orderMessageEnable = false;
}
