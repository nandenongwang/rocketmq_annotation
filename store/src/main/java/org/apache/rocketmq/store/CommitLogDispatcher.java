package org.apache.rocketmq.store;

/**
 * commitlog分派服务
 * reput服务中将新消息分派到不同的分派服务中构建检索索引(index)、消费索引(consumequeue)、过滤位图
 */
public interface CommitLogDispatcher {

    void dispatch(DispatchRequest request);
}
