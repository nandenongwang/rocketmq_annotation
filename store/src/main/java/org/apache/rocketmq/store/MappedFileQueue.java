package org.apache.rocketmq.store;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 映射文件队列
 * 聚合了目录下多个逻辑连续的映射文件
 */
public class MappedFileQueue {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private static final InternalLogger LOG_ERROR = InternalLoggerFactory.getLogger(LoggerName.STORE_ERROR_LOGGER_NAME);

    /**
     * 清理过期数据文件时每次最多删除多少文件数
     */
    private static final int DELETE_FILES_BATCH_MAX = 10;

    /**
     * 被映射文件所在目录
     */
    private final String storePath;

    /**
     * 单个映射文件大小
     */
    @Getter
    private final int mappedFileSize;

    /**
     * 目录下所有被映射文件
     */
    @Getter
    private final CopyOnWriteArrayList<MappedFile> mappedFiles = new CopyOnWriteArrayList<>();

    /**
     * 用于创建新的mappedFile
     */
    private final AllocateMappedFileService allocateMappedFileService;

    /**
     * 文件逻辑刷盘位置
     */
    @Getter
    @Setter
    private long flushedWhere = 0;

    /**
     * 文件逻辑提交位置 【仅物理日志使用写缓冲】
     */
    @Getter
    @Setter
    private long committedWhere = 0;

    /**
     * 最近确定刷盘时间 【flushLeastPages = 0】
     */
    @Getter
    private volatile long storeTimestamp = 0;

    public MappedFileQueue(String storePath, int mappedFileSize, AllocateMappedFileService allocateMappedFileService) {
        this.storePath = storePath;
        this.mappedFileSize = mappedFileSize;
        this.allocateMappedFileService = allocateMappedFileService;
    }

    /**
     * 检查文件数据是否正常 【仅检查文件名对应物理offset之差是否等于文件大小】
     */
    public void checkSelf() {

        if (!this.mappedFiles.isEmpty()) {
            Iterator<MappedFile> iterator = mappedFiles.iterator();
            MappedFile pre = null;
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();

                if (pre != null) {
                    if (cur.getFileFromOffset() - pre.getFileFromOffset() != this.mappedFileSize) {
                        LOG_ERROR.error("[BUG]The mappedFile queue's data is damaged, the adjacent mappedFile's offset don't match. pre file {}, cur file {}",
                                pre.getFileName(), cur.getFileName());
                    }
                }
                pre = cur;
            }
        }
    }

    /**
     * 查询指定数据项存储时间数据所在mappedFile【遍历查找最后修改时间大于该存储时间的文件、该数据必在此mappedFile中】
     */
    public MappedFile getMappedFileByTime(long timestamp) {
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs) {
            return null;
        }

        for (Object mf : mfs) {
            MappedFile mappedFile = (MappedFile) mf;
            if (mappedFile.getLastModifiedTimestamp() >= timestamp) {
                return mappedFile;
            }
        }

        return (MappedFile) mfs[mfs.length - 1];
    }

    /**
     * 获取所有mappedFile数组
     */
    private Object[] copyMappedFiles(int reservedMappedFiles/* 保留文件数 默认0 */) {
        Object[] mfs;

        if (this.mappedFiles.size() <= reservedMappedFiles) {
            return null;
        }

        mfs = this.mappedFiles.toArray();
        return mfs;
    }

    /**
     * 将加载映射文件队列存储目录下所有文件加载成mappedFile 【单个映射文件的读写、刷新、提交位置均初始化为文件大小即文件末尾】
     */
    public boolean load() {
        File dir = new File(this.storePath);
        File[] files = dir.listFiles();
        if (files != null) {
            // ascending order
            Arrays.sort(files);
            for (File file : files) {

                if (file.length() != this.mappedFileSize) {
                    log.warn(file + "\t" + file.length()
                            + " length not matched message store config value, please check it manually");
                    return false;
                }

                try {
                    MappedFile mappedFile = new MappedFile(file.getPath(), mappedFileSize);

                    mappedFile.setWrotePosition(this.mappedFileSize);
                    mappedFile.setFlushedPosition(this.mappedFileSize);
                    mappedFile.setCommittedPosition(this.mappedFileSize);
                    this.mappedFiles.add(mappedFile);
                    log.info("load " + file.getPath() + " OK");
                } catch (IOException e) {
                    log.error("load file " + file + " error", e);
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * 还有多少字节数据未刷盘
     */
    public long howMuchFallBehind() {
        if (this.mappedFiles.isEmpty()) {
            return 0;
        }

        long committed = this.flushedWhere;
        if (committed != 0) {
            MappedFile mappedFile = this.getLastMappedFile(0, false);
            if (mappedFile != null) {
                return (mappedFile.getFileFromOffset() + mappedFile.getWrotePosition()) - committed;
            }
        }

        return 0;
    }

    /**
     * 获取最后一个可写mappedFile 【不存在则根据startOffset新创建】
     */
    public MappedFile getLastMappedFile(long startOffset) {
        return getLastMappedFile(startOffset, true);
    }

    /**
     * 获取最后一个可写mappedFile 【startOffset用于计算不存在任何mappedFile时的文件起始offset 默认0 】
     */
    public MappedFile getLastMappedFile(long startOffset/* */, boolean needCreate) {
        long createOffset = -1;
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast == null) {
            createOffset = startOffset - (startOffset % this.mappedFileSize);
        }

        if (mappedFileLast != null && mappedFileLast.isFull()) {
            createOffset = mappedFileLast.getFileFromOffset() + this.mappedFileSize;
        }

        //是否要创建新日志文件
        if (createOffset != -1/* createOffset未变更、即最后一个mappedFile存在且未写满 */ && needCreate) {
            //创建新的mappedFile
            String nextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset);
            String nextNextFilePath = this.storePath + File.separator + UtilAll.offset2FileName(createOffset + this.mappedFileSize);
            MappedFile mappedFile = null;

            if (this.allocateMappedFileService != null) {
                mappedFile = this.allocateMappedFileService.putRequestAndReturnMappedFile(nextFilePath, nextNextFilePath, this.mappedFileSize);
            } else {
                try {
                    mappedFile = new MappedFile(nextFilePath, this.mappedFileSize);
                } catch (IOException e) {
                    log.error("create mappedFile exception", e);
                }
            }

            if (mappedFile != null) {
                if (this.mappedFiles.isEmpty()) {
                    mappedFile.setFirstCreateInQueue(true);
                }
                this.mappedFiles.add(mappedFile);
            }

            return mappedFile;
        }
        //返回新创建mappedFile或已存在且未写满的mappedFile
        return mappedFileLast;
    }

    /**
     * 获取最近的mappedFile
     */
    public MappedFile getLastMappedFile() {
        MappedFile mappedFileLast = null;

        while (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileLast = this.mappedFiles.get(this.mappedFiles.size() - 1);
                break;
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getLastMappedFile has exception.", e);
                break;
            }
        }

        return mappedFileLast;
    }

    /**
     * 删除最近的mappedFile
     */
    public void deleteLastMappedFile() {
        MappedFile lastMappedFile = getLastMappedFile();
        if (lastMappedFile != null) {
            lastMappedFile.destroy(1000);
            this.mappedFiles.remove(lastMappedFile);
            log.info("on recover, destroy a logic mapped file " + lastMappedFile.getFileName());

        }
    }

    /**
     * 获取最早的mappedFile
     */
    public MappedFile getFirstMappedFile() {
        MappedFile mappedFileFirst = null;

        if (!this.mappedFiles.isEmpty()) {
            try {
                mappedFileFirst = this.mappedFiles.get(0);
            } catch (IndexOutOfBoundsException e) {
                //ignore
            } catch (Exception e) {
                log.error("getFirstMappedFile has exception.", e);
            }
        }

        return mappedFileFirst;
    }

    /**
     * 重置mappedFile最大读写位置 【移除offset之后的文件、并将当前文件读写、刷新、提交位置都设为指定offset(当前文件指定位置后的重复利用时会被覆盖)】
     */
    public boolean resetOffset(long offset) {
        MappedFile mappedFileLast = getLastMappedFile();

        if (mappedFileLast != null) {
            long lastOffset = mappedFileLast.getFileFromOffset() + mappedFileLast.getWrotePosition();
            long diff = lastOffset - offset;

            final int maxDiff = this.mappedFileSize * 2;
            if (diff > maxDiff) {
                return false;
            }
        }

        ListIterator<MappedFile> iterator = this.mappedFiles.listIterator();

        while (iterator.hasPrevious()) {
            mappedFileLast = iterator.previous();
            if (offset >= mappedFileLast.getFileFromOffset()) {
                int where = (int) (offset % mappedFileLast.getFileSize());
                mappedFileLast.setFlushedPosition(where);
                mappedFileLast.setWrotePosition(where);
                mappedFileLast.setCommittedPosition(where);
                break;
            } else {
                iterator.remove();
            }
        }
        return true;
    }

    /**
     * 查询最小物理offset 【第一个mappedFile的fileFromOffset】
     */
    public long getMinOffset() {

        if (!this.mappedFiles.isEmpty()) {
            try {
                return this.mappedFiles.get(0).getFileFromOffset();
            } catch (IndexOutOfBoundsException e) {
                //continue;
            } catch (Exception e) {
                log.error("getMinOffset has exception.", e);
            }
        }
        return -1;
    }

    /**
     * 查询最大物理offset 【最后一个mappedFile的fileFromOffset + 可读数据position、可能部分还未刷盘】
     */
    public long getMaxOffset() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getReadPosition();
        }
        return 0;
    }

    /**
     * 获取最大写入物理offset
     */
    public long getMaxWrotePosition() {
        MappedFile mappedFile = getLastMappedFile();
        if (mappedFile != null) {
            return mappedFile.getFileFromOffset() + mappedFile.getWrotePosition();
        }
        return 0;
    }

    /**
     * 计算还有多少字节数据未提交
     */
    public long remainHowManyDataToCommit() {
        return getMaxWrotePosition() - committedWhere;
    }

    /**
     * 计算还有多少字节数据未刷盘
     */
    public long remainHowManyDataToFlush() {
        return getMaxOffset() - flushedWhere;
    }

    /**
     * 从将指定映射文件从被mappedqueue管理的mappedFile列表中移除 【过期文件销毁后】
     */
    void deleteExpiredFile(List<MappedFile> files) {

        if (!files.isEmpty()) {

            Iterator<MappedFile> iterator = files.iterator();
            while (iterator.hasNext()) {
                MappedFile cur = iterator.next();
                if (!this.mappedFiles.contains(cur)) {
                    iterator.remove();
                    log.info("This mappedFile {} is not contained by mappedFiles, so skip it.", cur.getFileName());
                }
            }

            try {
                if (!this.mappedFiles.removeAll(files)) {
                    log.error("deleteExpiredFile remove failed.");
                }
            } catch (Exception e) {
                log.error("deleteExpiredFile has exception.", e);
            }
        }
    }

    /**
     * 丢弃指定offset后异常数据 【找到所在mappedFile 设置读写、提交、刷新位置为该offset&文件内之后数据会重复使用&之后的文件直接销毁】
     */
    public void truncateDirtyFiles(long offset) {
        List<MappedFile> willRemoveFiles = new ArrayList<>();

        for (MappedFile file : this.mappedFiles) {
            long fileTailOffset = file.getFileFromOffset() + this.mappedFileSize;
            if (fileTailOffset > offset) {
                if (offset >= file.getFileFromOffset()) {
                    file.setWrotePosition((int) (offset % this.mappedFileSize));
                    file.setCommittedPosition((int) (offset % this.mappedFileSize));
                    file.setFlushedPosition((int) (offset % this.mappedFileSize));
                } else {
                    file.destroy(1000);
                    willRemoveFiles.add(file);
                }
            }
        }

        this.deleteExpiredFile(willRemoveFiles);
    }

    /**
     * 根据保留时间删除过期文件 【cleanImmediately为true会删除所有文件？？？】
     */
    public int deleteExpiredFileByTime(long expiredTime/* 保留时间默认72小时 */, int deleteFilesInterval/* 删除文件间隔 */, long intervalForcibly, boolean cleanImmediately) {
        //只要过期 全部删除不保留文件
        Object[] mfs = this.copyMappedFiles(0);

        if (null == mfs) {
            return 0;
        }

        int mfsLength = mfs.length - 1;
        int deleteCount = 0;
        List<MappedFile> willDeleteFiles = new ArrayList<>();
        for (int i = 0; i < mfsLength; i++) {
            MappedFile mappedFile = (MappedFile) mfs[i];
            long liveMaxTimestamp = mappedFile.getLastModifiedTimestamp() + expiredTime;
            //文件是否过期或立即清理
            //cleanImmediately为true会全部清理 删除一个后是否应再次判断磁盘使用率？？？TODO
            if (System.currentTimeMillis() >= liveMaxTimestamp || cleanImmediately) {
                if (mappedFile.destroy(intervalForcibly)) {
                    willDeleteFiles.add(mappedFile);
                    deleteCount++;
                    //最大删除文件数限制
                    if (willDeleteFiles.size() >= DELETE_FILES_BATCH_MAX) {
                        break;
                    }
                    //删除文件间隔
                    if (deleteFilesInterval > 0 && (i + 1) < mfsLength) {
                        try {
                            Thread.sleep(deleteFilesInterval);
                        } catch (InterruptedException ignored) {
                        }
                    }
                } else {
                    break;
                }
            } else {
                //avoid deleting files in the middle
                break;
            }
        }
        //清理内存结构
        deleteExpiredFile(willDeleteFiles);

        return deleteCount;
    }

    /**
     * 销毁过期的consumequeue映射文件 【读取mappedfile最后20字节的消费索引获取物理offset < 指定offset则该消费队列映射文件已过期】
     */
    public int deleteExpiredFileByOffset(long offset/* 默认最小物理offset */, int unitSize /* 默认消费索引单元大小 20字节 */) {
        Object[] mfs = this.copyMappedFiles(0);

        List<MappedFile> files = new ArrayList<>();
        int deleteCount = 0;
        if (null != mfs) {

            int mfsLength = mfs.length - 1;

            for (int i = 0; i < mfsLength; i++) {
                boolean destroy;
                MappedFile mappedFile = (MappedFile) mfs[i];
                SelectMappedBufferResult result = mappedFile.selectMappedBuffer(this.mappedFileSize - unitSize);
                if (result != null) {
                    long maxOffsetInLogicQueue = result.getByteBuffer().getLong();
                    result.release();
                    destroy = maxOffsetInLogicQueue < offset;
                    if (destroy) {
                        log.info("physic min offset " + offset + ", logics in current mappedFile max offset "
                                + maxOffsetInLogicQueue + ", delete it");
                    }
                } else if (!mappedFile.isAvailable()) { // Handle hanged file.
                    log.warn("Found a hanged consume queue file, attempting to delete it.");
                    destroy = true;
                } else {
                    log.warn("this being not executed forever.");
                    break;
                }

                if (destroy && mappedFile.destroy(1000 * 60)) {
                    files.add(mappedFile);
                    deleteCount++;
                } else {
                    break;
                }
            }
        }

        deleteExpiredFile(files);

        return deleteCount;
    }

    /**
     * 将脏数据刷盘、并更新mappedFile刷盘位置
     */
    public boolean flush(int flushLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.flushedWhere, this.flushedWhere == 0);
        if (mappedFile != null) {
            long tmpTimeStamp = mappedFile.getStoreTimestamp();
            int offset = mappedFile.flush(flushLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.flushedWhere;
            this.flushedWhere = where;
            if (0 == flushLeastPages) {
                this.storeTimestamp = tmpTimeStamp;
            }
        }

        return result;
    }

    /**
     * 提交写缓冲中脏数据、并更新mappedFile的提交位置
     */
    public boolean commit(int commitLeastPages) {
        boolean result = true;
        MappedFile mappedFile = this.findMappedFileByOffset(this.committedWhere, this.committedWhere == 0);
        if (mappedFile != null) {
            int offset = mappedFile.commit(commitLeastPages);
            long where = mappedFile.getFileFromOffset() + offset;
            result = where == this.committedWhere;
            this.committedWhere = where;
        }
        return result;
    }

    /**
     * 查找物理offset在哪个mappedFile中
     */
    public MappedFile findMappedFileByOffset(long offset, boolean returnFirstOnNotFound) {
        try {
            MappedFile firstMappedFile = this.getFirstMappedFile();
            MappedFile lastMappedFile = this.getLastMappedFile();
            if (firstMappedFile != null && lastMappedFile != null) {
                //region offset非法 【小于第一个起始offset大于最后一个结尾offset】
                if (offset < firstMappedFile.getFileFromOffset() || offset >= lastMappedFile.getFileFromOffset() + this.mappedFileSize) {
                    LOG_ERROR.warn("Offset not matched. Request offset: {}, firstOffset: {}, lastOffset: {}, mappedFileSize: {}, mappedFiles count: {}",
                            offset,
                            firstMappedFile.getFileFromOffset(),
                            lastMappedFile.getFileFromOffset() + this.mappedFileSize,
                            this.mappedFileSize,
                            this.mappedFiles.size());
                }
                //endregion
                else {
                    int index = (int) ((offset / this.mappedFileSize) - (firstMappedFile.getFileFromOffset() / this.mappedFileSize));
                    MappedFile targetFile = null;
                    try {
                        targetFile = this.mappedFiles.get(index);
                    } catch (Exception ignored) {
                    }
                    //region 检查查询offset是否在这个mappedFile范围内
                    if (targetFile != null && offset >= targetFile.getFileFromOffset() && offset < targetFile.getFileFromOffset() + this.mappedFileSize) {
                        return targetFile;
                    }
                    //endregion

                    //region 遍历所有mappedFilez挨个查找
                    for (MappedFile tmpMappedFile : this.mappedFiles) {
                        if (offset >= tmpMappedFile.getFileFromOffset() && offset < tmpMappedFile.getFileFromOffset() + this.mappedFileSize) {
                            return tmpMappedFile;
                        }
                    }
                    //endregion
                }

                if (returnFirstOnNotFound) {
                    return firstMappedFile;
                }
            }
        } catch (Exception e) {
            log.error("findMappedFileByOffset Exception", e);
        }

        return null;
    }

    /**
     * 查找物理offset在哪个mappedFile中、未找到返回空
     */
    public MappedFile findMappedFileByOffset(long offset) {
        return findMappedFileByOffset(offset, false);
    }

    /**
     * 获取映射内存总和 【所有映射文件大小求和】
     */
    public long getMappedMemorySize() {
        long size = 0;

        Object[] mfs = this.copyMappedFiles(0);
        if (mfs != null) {
            for (Object mf : mfs) {
                if (((ReferenceResource) mf).isAvailable()) {
                    size += this.mappedFileSize;
                }
            }
        }

        return size;
    }

    /**
     * 重试销毁并删除过期文件 【过期文件必定在队头、且首次删除时可能有引用导致删除完成、由清理任务重试】
     */
    public boolean retryDeleteFirstFile(long intervalForcibly) {
        MappedFile mappedFile = this.getFirstMappedFile();
        if (mappedFile != null) {
            if (!mappedFile.isAvailable()) {
                log.warn("the mappedFile was destroyed once, but still alive, " + mappedFile.getFileName());
                //销毁mappedFile 清理映射内存并删除文件
                boolean result = mappedFile.destroy(intervalForcibly);
                if (result) {
                    log.info("the mappedFile re delete OK, " + mappedFile.getFileName());
                    //清理被管理的内存数据结构
                    List<MappedFile> tmpFiles = new ArrayList<>();
                    tmpFiles.add(mappedFile);
                    this.deleteExpiredFile(tmpFiles);
                } else {
                    log.warn("the mappedFile re delete failed, " + mappedFile.getFileName());
                }

                return result;
            }
        }

        return false;
    }

    /**
     * 关闭
     */
    public void shutdown(long intervalForcibly) {
        for (MappedFile mf : this.mappedFiles) {
            mf.shutdown(intervalForcibly);
        }
    }

    /**
     * 销毁所有被管理mappedFile并删除该目录
     */
    public void destroy() {
        for (MappedFile mf : this.mappedFiles) {
            mf.destroy(1000 * 3);
        }
        this.mappedFiles.clear();
        this.flushedWhere = 0;

        // delete parent directory
        File file = new File(storePath);
        if (file.isDirectory()) {
            file.delete();
        }
    }
}
