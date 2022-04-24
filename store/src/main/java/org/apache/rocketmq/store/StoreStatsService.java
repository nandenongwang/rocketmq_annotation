package org.apache.rocketmq.store;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 每秒记录 messagestore put、get等操作次数
 * 用于 打印输出 & 获取messagesotre 运行状态
 */
public class StoreStatsService extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //采样频率
    private static final int FREQUENCY_OF_SAMPLING = 1000;

    private static final int MAX_RECORDS_OF_SAMPLING = 60 * 10;

    private static final String[] PUT_MESSAGE_ENTIRE_TIME_MAX_DESC = new String[]{
            "[<=0ms]", "[0~10ms]", "[10~50ms]", "[50~100ms]", "[100~200ms]", "[200~500ms]", "[500ms~1s]", "[1~2s]", "[2~3s]", "[3~4s]", "[4~5s]", "[5~10s]", "[10s~]",
    };

    private static int printTPSInterval = 60 * 1;

    @Getter
    private final AtomicLong putMessageFailedTimes = new AtomicLong(0);

    @Getter
    private final ConcurrentMap<String, AtomicLong> putMessageTopicTimesTotal = new ConcurrentHashMap<>(128);
    @Getter
    private final ConcurrentMap<String, AtomicLong> putMessageTopicSizeTotal = new ConcurrentHashMap<>(128);
    @Getter
    private final AtomicLong getMessageTimesTotalFound = new AtomicLong(0);
    @Getter
    private final AtomicLong getMessageTransferedMsgCount = new AtomicLong(0);
    @Getter
    private final AtomicLong getMessageTimesTotalMiss = new AtomicLong(0);

    private final LinkedList<CallSnapshot> putTimesList = new LinkedList<>();

    private final LinkedList<CallSnapshot> getTimesFoundList = new LinkedList<>();
    private final LinkedList<CallSnapshot> getTimesMissList = new LinkedList<>();
    private final LinkedList<CallSnapshot> transferedMsgCountList = new LinkedList<>();
    private volatile AtomicLong[] putMessageDistributeTime;
    private final long messageStoreBootTimestamp = System.currentTimeMillis();
    @Getter
    private volatile long putMessageEntireTimeMax = 0;
    private volatile long getMessageEntireTimeMax = 0;
    // for putMessageEntireTimeMax
    private ReentrantLock lockPut = new ReentrantLock();
    // for getMessageEntireTimeMax
    private ReentrantLock lockGet = new ReentrantLock();

    private volatile long dispatchMaxBuffer = 0;

    private ReentrantLock lockSampling = new ReentrantLock();
    private long lastPrintTimestamp = System.currentTimeMillis();

    public StoreStatsService() {
        this.initPutMessageDistributeTime();
    }

    private AtomicLong[] initPutMessageDistributeTime() {
        AtomicLong[] next = new AtomicLong[13];
        for (int i = 0; i < next.length; i++) {
            next[i] = new AtomicLong(0);
        }

        AtomicLong[] old = this.putMessageDistributeTime;

        this.putMessageDistributeTime = next;

        return old;
    }

    public void setPutMessageEntireTimeMax(long value) {
        final AtomicLong[] times = this.putMessageDistributeTime;

        if (null == times) {
            return;
        }

        // us
        if (value <= 0) {
            times[0].incrementAndGet();
        } else if (value < 10) {
            times[1].incrementAndGet();
        } else if (value < 50) {
            times[2].incrementAndGet();
        } else if (value < 100) {
            times[3].incrementAndGet();
        } else if (value < 200) {
            times[4].incrementAndGet();
        } else if (value < 500) {
            times[5].incrementAndGet();
        } else if (value < 1000) {
            times[6].incrementAndGet();
        }
        // 2s
        else if (value < 2000) {
            times[7].incrementAndGet();
        }
        // 3s
        else if (value < 3000) {
            times[8].incrementAndGet();
        }
        // 4s
        else if (value < 4000) {
            times[9].incrementAndGet();
        }
        // 5s
        else if (value < 5000) {
            times[10].incrementAndGet();
        }
        // 10s
        else if (value < 10000) {
            times[11].incrementAndGet();
        } else {
            times[12].incrementAndGet();
        }

        if (value > this.putMessageEntireTimeMax) {
            this.lockPut.lock();
            this.putMessageEntireTimeMax =
                    value > this.putMessageEntireTimeMax ? value : this.putMessageEntireTimeMax;
            this.lockPut.unlock();
        }
    }

    public long getGetMessageEntireTimeMax() {
        return getMessageEntireTimeMax;
    }

    public void setGetMessageEntireTimeMax(long value) {
        if (value > this.getMessageEntireTimeMax) {
            this.lockGet.lock();
            this.getMessageEntireTimeMax =
                    value > this.getMessageEntireTimeMax ? value : this.getMessageEntireTimeMax;
            this.lockGet.unlock();
        }
    }

    public long getDispatchMaxBuffer() {
        return dispatchMaxBuffer;
    }

    public void setDispatchMaxBuffer(long value) {
        this.dispatchMaxBuffer = value > this.dispatchMaxBuffer ? value : this.dispatchMaxBuffer;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(1024);
        Long totalTimes = getPutMessageTimesTotal();
        if (0 == totalTimes) {
            totalTimes = 1L;
        }

        sb.append("\truntime: " + this.getFormatRuntime() + "\r\n");
        sb.append("\tputMessageEntireTimeMax: " + this.putMessageEntireTimeMax + "\r\n");
        sb.append("\tputMessageTimesTotal: " + totalTimes + "\r\n");
        sb.append("\tputMessageSizeTotal: " + this.getPutMessageSizeTotal() + "\r\n");
        sb.append("\tputMessageDistributeTime: " + this.getPutMessageDistributeTimeStringInfo(totalTimes)
                + "\r\n");
        sb.append("\tputMessageAverageSize: " + (this.getPutMessageSizeTotal() / totalTimes.doubleValue())
                + "\r\n");
        sb.append("\tdispatchMaxBuffer: " + this.dispatchMaxBuffer + "\r\n");
        sb.append("\tgetMessageEntireTimeMax: " + this.getMessageEntireTimeMax + "\r\n");
        sb.append("\tputTps: " + this.getPutTps() + "\r\n");
        sb.append("\tgetFoundTps: " + this.getGetFoundTps() + "\r\n");
        sb.append("\tgetMissTps: " + this.getGetMissTps() + "\r\n");
        sb.append("\tgetTotalTps: " + this.getGetTotalTps() + "\r\n");
        sb.append("\tgetTransferedTps: " + this.getGetTransferedTps() + "\r\n");
        return sb.toString();
    }

    public long getPutMessageTimesTotal() {
        long rs = 0;
        for (AtomicLong data : putMessageTopicTimesTotal.values()) {
            rs += data.get();
        }
        return rs;
    }

    private String getFormatRuntime() {
        final long millisecond = 1;
        final long second = 1000 * millisecond;
        final long minute = 60 * second;
        final long hour = 60 * minute;
        final long day = 24 * hour;
        final MessageFormat messageFormat = new MessageFormat("[ {0} days, {1} hours, {2} minutes, {3} seconds ]");

        long time = System.currentTimeMillis() - this.messageStoreBootTimestamp;
        long days = time / day;
        long hours = (time % day) / hour;
        long minutes = (time % hour) / minute;
        long seconds = (time % minute) / second;
        return messageFormat.format(new Long[]{days, hours, minutes, seconds});
    }

    public long getPutMessageSizeTotal() {
        long rs = 0;
        for (AtomicLong data : putMessageTopicSizeTotal.values()) {
            rs += data.get();
        }
        return rs;
    }

    private String getPutMessageDistributeTimeStringInfo(Long total) {
        return this.putMessageDistributeTimeToString();
    }

    private String getPutTps() {
        return this.getPutTps(10) + " " + this.getPutTps(60) + " " + this.getPutTps(600);
    }

    private String getGetFoundTps() {
        return this.getGetFoundTps(10) + " " + this.getGetFoundTps(60) + " " + this.getGetFoundTps(600);
    }

    private String getGetMissTps() {
        return this.getGetMissTps(10) + " " + this.getGetMissTps(60) + " " + this.getGetMissTps(600);
    }

    private String getGetTotalTps() {
        return this.getGetTotalTps(10) + " " + this.getGetTotalTps(60) + " " + this.getGetTotalTps(600);
    }

    private String getGetTransferedTps() {
        return this.getGetTransferedTps(10) + " " + this.getGetTransferedTps(60) + " " + this.getGetTransferedTps(600);
    }

    private String putMessageDistributeTimeToString() {
        final AtomicLong[] times = this.putMessageDistributeTime;
        if (null == times) {
            return null;
        }

        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times.length; i++) {
            long value = times[i].get();
            sb.append(String.format("%s:%d", PUT_MESSAGE_ENTIRE_TIME_MAX_DESC[i], value));
            sb.append(" ");
        }

        return sb.toString();
    }

    private String getPutTps(int time) {
        String result = "";
        this.lockSampling.lock();
        try {
            CallSnapshot last = this.putTimesList.getLast();

            if (this.putTimesList.size() > time) {
                CallSnapshot lastBefore = this.putTimesList.get(this.putTimesList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.lockSampling.unlock();
        }
        return result;
    }

    private String getGetFoundTps(int time) {
        String result = "";
        this.lockSampling.lock();
        try {
            CallSnapshot last = this.getTimesFoundList.getLast();

            if (this.getTimesFoundList.size() > time) {
                CallSnapshot lastBefore =
                        this.getTimesFoundList.get(this.getTimesFoundList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }
        } finally {
            this.lockSampling.unlock();
        }

        return result;
    }

    private String getGetMissTps(int time) {
        String result = "";
        this.lockSampling.lock();
        try {
            CallSnapshot last = this.getTimesMissList.getLast();

            if (this.getTimesMissList.size() > time) {
                CallSnapshot lastBefore =
                        this.getTimesMissList.get(this.getTimesMissList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.lockSampling.unlock();
        }

        return result;
    }

    private String getGetTotalTps(int time) {
        this.lockSampling.lock();
        double found = 0;
        double miss = 0;
        try {
            {
                CallSnapshot last = this.getTimesFoundList.getLast();

                if (this.getTimesFoundList.size() > time) {
                    CallSnapshot lastBefore =
                            this.getTimesFoundList.get(this.getTimesFoundList.size() - (time + 1));
                    found = CallSnapshot.getTPS(lastBefore, last);
                }
            }
            {
                CallSnapshot last = this.getTimesMissList.getLast();

                if (this.getTimesMissList.size() > time) {
                    CallSnapshot lastBefore =
                            this.getTimesMissList.get(this.getTimesMissList.size() - (time + 1));
                    miss = CallSnapshot.getTPS(lastBefore, last);
                }
            }

        } finally {
            this.lockSampling.unlock();
        }

        return Double.toString(found + miss);
    }

    private String getGetTransferedTps(int time) {
        String result = "";
        this.lockSampling.lock();
        try {
            CallSnapshot last = this.transferedMsgCountList.getLast();

            if (this.transferedMsgCountList.size() > time) {
                CallSnapshot lastBefore =
                        this.transferedMsgCountList.get(this.transferedMsgCountList.size() - (time + 1));
                result += CallSnapshot.getTPS(lastBefore, last);
            }

        } finally {
            this.lockSampling.unlock();
        }

        return result;
    }

    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = new HashMap<String, String>(64);

        long totalTimes = getPutMessageTimesTotal();
        if (0 == totalTimes) {
            totalTimes = 1L;
        }

        result.put("bootTimestamp", String.valueOf(this.messageStoreBootTimestamp));
        result.put("runtime", this.getFormatRuntime());
        result.put("putMessageEntireTimeMax", String.valueOf(this.putMessageEntireTimeMax));
        result.put("putMessageTimesTotal", String.valueOf(totalTimes));
        result.put("putMessageSizeTotal", String.valueOf(this.getPutMessageSizeTotal()));
        result.put("putMessageDistributeTime",
                String.valueOf(this.getPutMessageDistributeTimeStringInfo(totalTimes)));
        result.put("putMessageAverageSize",
                String.valueOf(this.getPutMessageSizeTotal() / (double) totalTimes));
        result.put("dispatchMaxBuffer", String.valueOf(this.dispatchMaxBuffer));
        result.put("getMessageEntireTimeMax", String.valueOf(this.getMessageEntireTimeMax));
        result.put("putTps", String.valueOf(this.getPutTps()));
        result.put("getFoundTps", String.valueOf(this.getGetFoundTps()));
        result.put("getMissTps", String.valueOf(this.getGetMissTps()));
        result.put("getTotalTps", String.valueOf(this.getGetTotalTps()));
        result.put("getTransferedTps", String.valueOf(this.getGetTransferedTps()));

        return result;
    }

    @Override
    public void run() {
        log.info(this.getServiceName() + " service started");

        while (!this.isStopped()) {
            try {
                this.waitForRunning(FREQUENCY_OF_SAMPLING);

                this.sampling();

                this.printTps();
            } catch (Exception e) {
                log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        log.info(this.getServiceName() + " service end");
    }

    @Override
    public String getServiceName() {
        return StoreStatsService.class.getSimpleName();
    }

    private void sampling() {
        this.lockSampling.lock();
        try {
            this.putTimesList.add(new CallSnapshot(System.currentTimeMillis(), getPutMessageTimesTotal()));
            if (this.putTimesList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.putTimesList.removeFirst();
            }

            this.getTimesFoundList.add(new CallSnapshot(System.currentTimeMillis(),
                    this.getMessageTimesTotalFound.get()));
            if (this.getTimesFoundList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.getTimesFoundList.removeFirst();
            }

            this.getTimesMissList.add(new CallSnapshot(System.currentTimeMillis(),
                    this.getMessageTimesTotalMiss.get()));
            if (this.getTimesMissList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.getTimesMissList.removeFirst();
            }

            this.transferedMsgCountList.add(new CallSnapshot(System.currentTimeMillis(),
                    this.getMessageTransferedMsgCount.get()));
            if (this.transferedMsgCountList.size() > (MAX_RECORDS_OF_SAMPLING + 1)) {
                this.transferedMsgCountList.removeFirst();
            }

        } finally {
            this.lockSampling.unlock();
        }
    }

    private void printTps() {
        if (System.currentTimeMillis() > (this.lastPrintTimestamp + printTPSInterval * 1000)) {
            this.lastPrintTimestamp = System.currentTimeMillis();

            log.info("[STORETPS] put_tps {} get_found_tps {} get_miss_tps {} get_transfered_tps {}",
                    this.getPutTps(printTPSInterval),
                    this.getGetFoundTps(printTPSInterval),
                    this.getGetMissTps(printTPSInterval),
                    this.getGetTransferedTps(printTPSInterval)
            );

            final AtomicLong[] times = this.initPutMessageDistributeTime();
            if (null == times) {
                return;
            }

            final StringBuilder sb = new StringBuilder();
            long totalPut = 0;
            for (int i = 0; i < times.length; i++) {
                long value = times[i].get();
                totalPut += value;
                sb.append(String.format("%s:%d", PUT_MESSAGE_ENTIRE_TIME_MAX_DESC[i], value));
                sb.append(" ");
            }

            log.info("[PAGECACHERT] TotalPut {}, PutMessageDistributeTime {}", totalPut, sb.toString());
        }
    }

    public AtomicLong getSinglePutMessageTopicSizeTotal(String topic) {
        AtomicLong rs = putMessageTopicSizeTotal.get(topic);
        if (null == rs) {
            rs = new AtomicLong(0);
            AtomicLong previous = putMessageTopicSizeTotal.putIfAbsent(topic, rs);
            if (previous != null) {
                rs = previous;
            }
        }
        return rs;
    }

    public AtomicLong getSinglePutMessageTopicTimesTotal(String topic) {
        AtomicLong rs = putMessageTopicTimesTotal.get(topic);
        if (null == rs) {
            rs = new AtomicLong(0);
            AtomicLong previous = putMessageTopicTimesTotal.putIfAbsent(topic, rs);
            if (previous != null) {
                rs = previous;
            }
        }
        return rs;
    }

    /**
     * 瞬时调用快照
     */
    @AllArgsConstructor
    static class CallSnapshot {

        /**
         * 调用时间戳
         */
        public final long timestamp;

        /**
         * 总调用次数
         */
        public final long callTimesTotal;

        /**
         * 计算两次瞬时快照时间段内指标tps
         */
        public static double getTPS(CallSnapshot begin, CallSnapshot end) {
            long total = end.callTimesTotal - begin.callTimesTotal;
            long time = end.timestamp - begin.timestamp;

            double tps = total / (double) time;

            return tps * 1000;
        }
    }
}
