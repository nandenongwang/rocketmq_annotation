package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import lombok.Data;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.common.message.MessageQueue;


/**
 * 将指定broker组上的queue平均分配 【指定方式：brokername.splite[1]在consumeridcs配置中】
 * 先将整除的分段 未整除的轮流分配
 */
@Data
public class AllocateMessageQueueByMachineRoom implements AllocateMessageQueueStrategy {

    private Set<String> consumeridcs;

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll, List<String> cidAll) {
        List<MessageQueue> result = new ArrayList<>();

        //当前消费者下线 不分配
        int currentIndex = cidAll.indexOf(currentCID);
        if (currentIndex < 0) {
            return result;
        }

        List<MessageQueue> premqAll = new ArrayList<>();
        for (MessageQueue mq : mqAll) {
            String[] temp = mq.getBrokerName().split("@");
            if (temp.length == 2 && consumeridcs.contains(temp[0])) {
                premqAll.add(mq);
            }

        }

        int mod = premqAll.size() / cidAll.size();
        int rem = premqAll.size() % cidAll.size();
        int startIndex = mod * currentIndex;
        int endIndex = startIndex + mod;
        for (int i = startIndex; i < endIndex; i++) {
            result.add(premqAll.get(i));
        }
        if (rem > currentIndex) {
            result.add(premqAll.get(currentIndex + mod * cidAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "MACHINE_ROOM";
    }
}
