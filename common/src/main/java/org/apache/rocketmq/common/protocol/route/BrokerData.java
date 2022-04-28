package org.apache.rocketmq.common.protocol.route;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.rocketmq.common.MixAll;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

@Data
@NoArgsConstructor
public class BrokerData implements Comparable<BrokerData> {

    /**
     * 集群名 默认default
     */
    private String cluster;

    /**
     * broker组名
     */
    private String brokerName;

    /**
     * broker组下各broker地址、master和slave
     */
    private HashMap<Long/* brokerId */, String/* broker address */> brokerAddrs;

    private final Random random = new Random();

    public BrokerData(String cluster, String brokerName, HashMap<Long, String> brokerAddrs) {
        this.cluster = cluster;
        this.brokerName = brokerName;
        this.brokerAddrs = brokerAddrs;
    }

    /**
     * 选取一组下broker地址、master优先、否者随机选取一salve
     * Selects a (preferably master) broker address from the registered list.
     * If the master's address cannot be found, a slave broker address is selected in a random manner.
     *
     * @return Broker address.
     */
    public String selectBrokerAddr() {
        String addr = this.brokerAddrs.get(MixAll.MASTER_ID);

        if (addr == null) {
            List<String> addrs = new ArrayList<>(brokerAddrs.values());
            return addrs.get(random.nextInt(addrs.size()));
        }

        return addr;
    }

    @Override
    public int compareTo(BrokerData o) {
        return this.brokerName.compareTo(o.getBrokerName());
    }

}
