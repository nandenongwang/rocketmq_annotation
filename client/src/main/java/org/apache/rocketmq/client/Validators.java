package org.apache.rocketmq.client;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.topic.TopicValidator;

/**
 * 校验工具类
 */
public class Validators {
    public static final String VALID_PATTERN_STR = "^[%|a-zA-Z0-9_-]+$";
    public static final Pattern PATTERN = Pattern.compile(VALID_PATTERN_STR);
    public static final int CHARACTER_MAX_LENGTH = 255;
    public static final int TOPIC_MAX_LENGTH = 127;

    /**
     * 在原始串中找到第一个符合给定正则匹配规则的子串
     */
    public static String getGroupWithRegularExpression(String origin, String patternStr) {
        Pattern pattern = Pattern.compile(patternStr);
        Matcher matcher = pattern.matcher(origin);
        while (matcher.find()) {
            return matcher.group(0);
        }
        return null;
    }

    /**
     * 检验group格式 【不为空、长度小于127、大小写字母数字_-组成】
     */
    public static void checkGroup(String group) throws MQClientException {
        if (UtilAll.isBlank(group)) {
            throw new MQClientException("the specified group is blank", null);
        }

        if (group.length() > CHARACTER_MAX_LENGTH) {
            throw new MQClientException("the specified group is longer than group max length 255.", null);
        }

        if (!regularExpressionMatcher(group, PATTERN)) {
            throw new MQClientException(String.format(
                    "the specified group[%s] contains illegal characters, allowing only %s", group,
                    VALID_PATTERN_STR), null);
        }

    }

    /**
     * 校验字符串是否与给定正则pattern匹配
     */
    public static boolean regularExpressionMatcher(String origin, Pattern pattern) {
        if (pattern == null) {
            return true;
        }
        Matcher matcher = pattern.matcher(origin);
        return matcher.matches();
    }

    /**
     * 校验消息格式 【消息是否为空、消息体是否为空、长度是否符合要求、topic格式、topic是否允许发送】
     */
    public static void checkMessage(Message msg, DefaultMQProducer defaultMQProducer) throws MQClientException {
        if (null == msg) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message is null");
        }
        // topic
        Validators.checkTopic(msg.getTopic());
        Validators.isNotAllowedSendTopic(msg.getTopic());

        // body
        if (null == msg.getBody()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body is null");
        }

        if (0 == msg.getBody().length) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body length is zero");
        }

        if (msg.getBody().length > defaultMQProducer.getMaxMessageSize()) {
            throw new MQClientException(ResponseCode.MESSAGE_ILLEGAL, "the message body size over max value, MAX: " + defaultMQProducer.getMaxMessageSize());
        }
    }

    /**
     * 校验topic格式是否合法 【不为空、长度小于127、大小写字母数字_-组成】
     */
    public static void checkTopic(String topic) throws MQClientException {
        if (UtilAll.isBlank(topic)) {
            throw new MQClientException("The specified topic is blank", null);
        }

        if (!regularExpressionMatcher(topic, PATTERN)) {
            throw new MQClientException(String.format(
                    "The specified topic[%s] contains illegal characters, allowing only %s", topic,
                    VALID_PATTERN_STR), null);
        }

        if (topic.length() > TOPIC_MAX_LENGTH) {
            throw new MQClientException(
                    String.format("The specified topic is longer than topic max length %d.", TOPIC_MAX_LENGTH), null);
        }
    }

    /**
     * 是否是系统topic 【包含在内置系统topic中或以rmq_sys_开头】
     */
    public static void isSystemTopic(String topic) throws MQClientException {
        if (TopicValidator.isSystemTopic(topic)) {
            throw new MQClientException(String.format("The topic[%s] is conflict with system topic.", topic), null);
        }
    }

    /**
     * 是否是不允许发送的topic 【默认 SCHEDULE_TOPIC_XXXX】
     */
    public static void isNotAllowedSendTopic(String topic) throws MQClientException {
        if (TopicValidator.isNotAllowedSendTopic(topic)) {
            throw new MQClientException(String.format("Sending message to topic[%s] is forbidden.", topic), null);
        }
    }

}
