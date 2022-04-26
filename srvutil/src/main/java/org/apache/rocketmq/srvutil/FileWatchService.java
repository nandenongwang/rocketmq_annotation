package org.apache.rocketmq.srvutil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

/**
 * 文件监听服务
 * 默认每500ms 比较监听文件hash值 文件改变则通知监听器
 */
public class FileWatchService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.COMMON_LOGGER_NAME);

    private final List<String/* 监听文件路径 */> watchFiles;
    private final List<String/* 监听文件hash值 */> fileCurrentHash;
    private final Listener listener;
    private static final int WATCH_INTERVAL = 500;
    private final MessageDigest md = MessageDigest.getInstance("MD5");

    public FileWatchService(String[] watchFiles, Listener listener) throws Exception {
        this.listener = listener;
        this.watchFiles = new ArrayList<>();
        this.fileCurrentHash = new ArrayList<>();

        for (String watchFile : watchFiles) {
            if (StringUtils.isNotEmpty(watchFile) && new File(watchFile).exists()) {
                this.watchFiles.add(watchFile);
                this.fileCurrentHash.add(hash(watchFile));
            }
        }
    }

    @Override
    public String getServiceName() {
        return "FileWatchService";
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(WATCH_INTERVAL);

                for (int i = 0; i < watchFiles.size(); i++) {
                    String newHash;
                    try {
                        newHash = hash(watchFiles.get(i));
                    } catch (Exception e) {
                        log.warn(this.getServiceName() + " service has exception when calculate the file hash. ", e);
                        continue;
                    }
                    if (!newHash.equals(fileCurrentHash.get(i))) {
                        fileCurrentHash.set(i, newHash);
                        listener.onChanged(watchFiles.get(i));
                    }
                }
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }
        log.info(this.getServiceName() + " service end");
    }

    private String hash(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        md.update(Files.readAllBytes(path));
        byte[] hash = md.digest();
        return UtilAll.bytes2string(hash);
    }

    public interface Listener {
        /**
         * Will be called when the target files are changed
         *
         * @param path the changed file path
         */
        void onChanged(String path);
    }
}
