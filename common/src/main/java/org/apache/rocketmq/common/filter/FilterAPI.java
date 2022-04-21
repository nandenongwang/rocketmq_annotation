package org.apache.rocketmq.common.filter;

import java.net.URL;

import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

public class FilterAPI {
    /**
     * 加载过滤类资源
     */
    @Deprecated
    public static URL classFile(final String className) {
        final String javaSource = simpleClassName(className) + ".java";
        return FilterAPI.class.getClassLoader().getResource(javaSource);
    }

    /**
     * com.xx.xx.testClass  -> testClass
     */
    public static String simpleClassName(final String className) {
        String simple = className;
        int index = className.lastIndexOf(".");
        if (index >= 0) {
            simple = className.substring(index + 1);
        }

        return simple;
    }

    /**
     * 创建tags订阅配置 【订阅topic、||切割tags 表达式】
     */
    public static SubscriptionData buildSubscriptionData(String consumerGroup, String topic, String subString) throws Exception {
        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        //默认订阅所有
        if (null == subString || subString.equals(SubscriptionData.SUB_ALL) || subString.length() == 0) {
            subscriptionData.setSubString(SubscriptionData.SUB_ALL);
        } else {
            String[] tags = subString.split("\\|\\|");
            if (tags.length > 0) {
                for (String tag : tags) {
                    if (tag.length() > 0) {
                        String trimString = tag.trim();
                        if (trimString.length() > 0) {
                            subscriptionData.getTagsSet().add(trimString);
                            subscriptionData.getCodeSet().add(trimString.hashCode());
                        }
                    }
                }
            } else {
                throw new Exception("subString split error");
            }
        }

        return subscriptionData;
    }

    /**
     * 创建订阅配置 【订阅topic、表达式类型、表达式内容】
     */
    public static SubscriptionData build(String topic, String subString, String type) throws Exception {
        if (ExpressionType.TAG.equals(type) || type == null) {
            return buildSubscriptionData(null, topic, subString);
        }

        if (subString == null || subString.length() < 1) {
            throw new IllegalArgumentException("Expression can't be null! " + type);
        }

        SubscriptionData subscriptionData = new SubscriptionData();
        subscriptionData.setTopic(topic);
        subscriptionData.setSubString(subString);
        subscriptionData.setExpressionType(type);

        return subscriptionData;
    }
}
