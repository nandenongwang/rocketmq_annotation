package org.apache.rocketmq.store;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

public class StoreUtil {
    public static final long TOTAL_PHYSICAL_MEMORY_SIZE = getTotalPhysicalMemorySize();

    /**
     * 获取总内存大小 【默认24G】
     */
    @SuppressWarnings("restriction")
    public static long getTotalPhysicalMemorySize() {
        long physicalTotal = 1024 * 1024 * 1024 * 24L;
        OperatingSystemMXBean osmxb = ManagementFactory.getOperatingSystemMXBean();
        if (osmxb instanceof com.sun.management.OperatingSystemMXBean) {
            physicalTotal = ((com.sun.management.OperatingSystemMXBean) osmxb).getTotalPhysicalMemorySize();
        }

        return physicalTotal;
    }
}
