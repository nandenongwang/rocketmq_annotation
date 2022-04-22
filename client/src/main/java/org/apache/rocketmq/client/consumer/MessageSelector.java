package org.apache.rocketmq.client.consumer;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.rocketmq.common.filter.ExpressionType;

/**
 * 消息筛选器 封装业务侧订阅配置 【过滤类型和过滤表达式】
 */
@AllArgsConstructor
@Data
public class MessageSelector {

    /**
     * 过滤类型 tags获取sql
     */
    private final String expressionType;

    /**
     * 过滤表达式
     */
    private final String expression;


    /**
     * Use SLQ92 to select message.
     *
     * @param sql if null or empty, will be treated as select all message.
     */
    public static MessageSelector bySql(String sql) {
        return new MessageSelector(ExpressionType.SQL92, sql);
    }

    /**
     * Use tag to select message.
     *
     * @param tag if null or empty or "*", will be treated as select all message.
     */
    public static MessageSelector byTag(String tag) {
        return new MessageSelector(ExpressionType.TAG, tag);
    }

}
