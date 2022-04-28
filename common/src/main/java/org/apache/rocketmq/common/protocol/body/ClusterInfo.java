package org.apache.rocketmq.common.protocol.body;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * 集群信息
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class ClusterInfo extends RemotingSerializable {

    /**
     * broker组下管理broker成员
     */
    private HashMap<String/* brokerName */, BrokerData> brokerAddrTable;

    /**
     * 集群下管理broker组
     */
    private HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    public String[] retrieveAllAddrByCluster(String cluster) {
        List<String> addrs = new ArrayList<>();
        if (clusterAddrTable.containsKey(cluster)) {
            Set<String> brokerNames = clusterAddrTable.get(cluster);
            for (String brokerName : brokerNames) {
                BrokerData brokerData = brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    addrs.addAll(brokerData.getBrokerAddrs().values());
                }
            }
        }

        return addrs.toArray(new String[]{});
    }

    public String[] retrieveAllClusterNames() {
        return clusterAddrTable.keySet().toArray(new String[]{});
    }
}
