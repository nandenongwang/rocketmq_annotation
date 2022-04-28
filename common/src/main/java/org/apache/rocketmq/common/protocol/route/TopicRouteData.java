package org.apache.rocketmq.common.protocol.route;

import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * topic路由配置
 */
@EqualsAndHashCode(callSuper = false)
@Data
public class TopicRouteData extends RemotingSerializable {

    /**
     * 顺序topic配置 默认空
     */
    private String orderTopicConf;

    /**
     * topic下queue配置 【所在broker组、读写queue数量】
     */
    private List<QueueData> queueDatas;

    /**
     * broker组下所有成员地址
     */
    private List<BrokerData> brokerDatas;

    /**
     * broker成员及其对应的过滤服务地址
     */
    private HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;

    public TopicRouteData cloneTopicRouteData() {
        TopicRouteData topicRouteData = new TopicRouteData();
        topicRouteData.setQueueDatas(new ArrayList<>());
        topicRouteData.setBrokerDatas(new ArrayList<>());
        topicRouteData.setFilterServerTable(new HashMap<>());
        topicRouteData.setOrderTopicConf(this.orderTopicConf);

        if (this.queueDatas != null) {
            topicRouteData.getQueueDatas().addAll(this.queueDatas);
        }

        if (this.brokerDatas != null) {
            topicRouteData.getBrokerDatas().addAll(this.brokerDatas);
        }

        if (this.filterServerTable != null) {
            topicRouteData.getFilterServerTable().putAll(this.filterServerTable);
        }

        return topicRouteData;
    }
}
