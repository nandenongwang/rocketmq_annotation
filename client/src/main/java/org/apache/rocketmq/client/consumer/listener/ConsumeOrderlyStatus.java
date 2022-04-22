package org.apache.rocketmq.client.consumer.listener;

/**
 * 顺序消费时业务消费返回消费状态
 * <p>
 * 自动提交时：
 * 成功|提交|回滚 清空顺序消费缓存tree并更新提交offset 【自动提交时消费失败返回回滚状态会造成消费结果丢失】
 * 挂起 延时该重新请求 【返回null也是挂起】
 * <p>
 * 手动提交时；
 * 成功 继续该请求 会将本次消费的消息积压在consumingMsgOrderlyTreeMap中跟下次消息一起提交或回滚【两次消费结果不一致会出现异常】
 * 提交 更新提交offset后继续该请求
 * 回滚 放回消息后延时重新请求
 * 挂起 延时重新请求 【返回null也是挂起】
 */
public enum ConsumeOrderlyStatus {
    /**
     * 成功
     */
    SUCCESS,
    /**
     * 回滚
     */
    @Deprecated
    ROLLBACK,
    /**
     * 提交
     */
    @Deprecated
    COMMIT,
    /**
     * 挂起
     */
    SUSPEND_CURRENT_QUEUE_A_MOMENT;
}
