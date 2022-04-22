package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * 未使用
 * 【1、2、3】 【0、1、2、3、4、5、6、7、8、9、10、11】
 * 0369 14710 258
 */
public class AllocateMessageQueueAveragelyByCircle implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        //region 配置前check
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<>();
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                    consumerGroup,
                    currentCID,
                    cidAll);
            return result;
        }
        //endregion

        //【1、2、3】 【0、1、2、3、4、5、6、7、8、9、10、11】 2
        int index = cidAll.indexOf(currentCID);//1
        for (int i = index; i < mqAll.size(); i++) {//i=1;i<12;i++
            if (i % cidAll.size()/* 3 */ == index) {
                result.add(mqAll.get(i));//1、4、7、10
            }
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG_BY_CIRCLE";
    }
}
