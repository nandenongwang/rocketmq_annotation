package org.apache.rocketmq.client.impl.consumer;

import lombok.Data;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费者拉取状态维护组件
 */
public class AssignedMessageQueue {

    /**
     * 所有指定消费的queue及其拉取状态对象
     */
    private final ConcurrentHashMap<MessageQueue, MessageQueueState> assignedMessageQueueState;

    /**
     * 消费者重平衡器
     */
    private RebalanceImpl rebalanceImpl;

    public AssignedMessageQueue() {
        assignedMessageQueueState = new ConcurrentHashMap<>();
    }

    /**
     * 设置消费者重平衡器
     */
    public void setRebalanceImpl(RebalanceImpl rebalanceImpl) {
        this.rebalanceImpl = rebalanceImpl;
    }

    /**
     * 获取该消费者所有被分配消费的queue
     */
    public Set<MessageQueue> messageQueues() {
        return assignedMessageQueueState.keySet();
    }

    /**
     * 对该queue消息拉取是否被暂停
     */
    public boolean isPaused(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.isPaused();
        }
        return true;
    }

    /**
     * 暂停对指定queue的消息拉取
     */
    public void pause(Collection<MessageQueue> messageQueues) {
        for (MessageQueue messageQueue : messageQueues) {
            MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            if (assignedMessageQueueState.get(messageQueue) != null) {
                messageQueueState.setPaused(true);
            }
        }
    }

    /**
     * 恢复对指定queue的消息拉取
     */
    public void resume(Collection<MessageQueue> messageQueueCollection) {
        for (MessageQueue messageQueue : messageQueueCollection) {
            MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            if (assignedMessageQueueState.get(messageQueue) != null) {
                messageQueueState.setPaused(false);
            }
        }
    }

    /**
     * 获取指定拉取queue的处理queue
     */
    public ProcessQueue getProcessQueue(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getProcessQueue();
        }
        return null;
    }

    /**
     * 获取指定queue下次拉取位置
     */
    public long getPullOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getPullOffset();
        }
        return -1;
    }

    /**
     * 更新指定queue下次拉取位置
     */
    public void updatePullOffset(MessageQueue messageQueue, long offset) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            messageQueueState.setPullOffset(offset);
        }
    }

    /**
     * 获取指定queue内部消费进度
     */
    public long getConsumerOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getConsumeOffset();
        }
        return -1;
    }

    /**
     * 更新指定queue内部消费进度
     */
    public void updateConsumeOffset(MessageQueue messageQueue, long offset) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            messageQueueState.setConsumeOffset(offset);
        }
    }

    /**
     * 设置指定queue指定拉取位置
     */
    public void setSeekOffset(MessageQueue messageQueue, long offset) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            messageQueueState.setSeekOffset(offset);
        }
    }

    /**
     * 获取指定queue指定拉取位置
     */
    public long getSeekOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getSeekOffset();
        }
        return -1;
    }

    /**
     * 重平衡后更新新分配指定消费queue的拉取状态对象管理
     */
    public void updateAssignedMessageQueue(String topic, Collection<MessageQueue> assigned) {
        synchronized (this.assignedMessageQueueState) {
            Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueueState.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, MessageQueueState> next = it.next();
                if (next.getKey().getTopic().equals(topic)) {
                    //过滤出已不属于该消费者消费的queue、设置处理queue dropped位并移除其状态管理
                    if (!assigned.contains(next.getKey())) {
                        next.getValue().getProcessQueue().setDropped(true);
                        it.remove();
                    }
                }
            }
            //重新添加新分配的queue
            addAssignedMessageQueue(assigned);
        }
    }

    //region 更新或添加指定消费的queue的拉取状态维护对象
    public void updateAssignedMessageQueue(Collection<MessageQueue> assigned) {
        synchronized (this.assignedMessageQueueState) {
            Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueueState.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, MessageQueueState> next = it.next();
                if (!assigned.contains(next.getKey())) {
                    next.getValue().getProcessQueue().setDropped(true);
                    it.remove();
                }
            }
            addAssignedMessageQueue(assigned);
        }
    }

    private void addAssignedMessageQueue(Collection<MessageQueue> assigned) {
        for (MessageQueue messageQueue : assigned) {
            if (!this.assignedMessageQueueState.containsKey(messageQueue)) {
                MessageQueueState messageQueueState;
                if (rebalanceImpl != null && rebalanceImpl.getProcessQueueTable().get(messageQueue) != null) {
                    messageQueueState = new MessageQueueState(messageQueue, rebalanceImpl.getProcessQueueTable().get(messageQueue));
                } else {
                    ProcessQueue processQueue = new ProcessQueue();
                    messageQueueState = new MessageQueueState(messageQueue, processQueue);
                }
                this.assignedMessageQueueState.put(messageQueue, messageQueueState);
            }
        }
    }
    //endregion

    /**
     * 移除指定topic的所有queue的拉取状态维护对象
     */
    public void removeAssignedMessageQueue(String topic) {
        synchronized (this.assignedMessageQueueState) {
            this.assignedMessageQueueState.entrySet().removeIf(next -> next.getKey().getTopic().equals(topic));
        }
    }

    /**
     * 获取该消费者所有被分配消费的queue
     */
    public Set<MessageQueue> getAssignedMessageQueues() {
        return this.assignedMessageQueueState.keySet();
    }

    /**
     * 指定消费queue的拉取状态
     */
    @Data
    private static class MessageQueueState {

        /**
         * 拉取queue
         */
        private MessageQueue messageQueue;

        /**
         * 处理queue
         */
        private ProcessQueue processQueue;

        /**
         * 是否暂停
         */
        private volatile boolean paused = false;

        /**
         * 下次pull开始位置
         */
        private volatile long pullOffset = -1;

        /**
         * 生产者本地消费进度
         */
        private volatile long consumeOffset = -1;

        /**
         * 指定开始pull位置
         */
        private volatile long seekOffset = -1;

        private MessageQueueState(MessageQueue messageQueue, ProcessQueue processQueue) {
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }
    }
}
