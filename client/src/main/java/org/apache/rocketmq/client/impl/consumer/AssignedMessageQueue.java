package org.apache.rocketmq.client.impl.consumer;

import lombok.Data;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 保存消费者所有分配的消费queue状态&处理queue
 * 重平后需更新
 */
public class AssignedMessageQueue {

    private final ConcurrentHashMap<MessageQueue, MessageQueueState> assignedMessageQueueState;

    private RebalanceImpl rebalanceImpl;

    public AssignedMessageQueue() {
        assignedMessageQueueState = new ConcurrentHashMap<>();
    }

    public void setRebalanceImpl(RebalanceImpl rebalanceImpl) {
        this.rebalanceImpl = rebalanceImpl;
    }

    public Set<MessageQueue> messageQueues() {
        return assignedMessageQueueState.keySet();
    }

    public boolean isPaused(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.isPaused();
        }
        return true;
    }

    public void pause(Collection<MessageQueue> messageQueues) {
        for (MessageQueue messageQueue : messageQueues) {
            MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            if (assignedMessageQueueState.get(messageQueue) != null) {
                messageQueueState.setPaused(true);
            }
        }
    }

    public void resume(Collection<MessageQueue> messageQueueCollection) {
        for (MessageQueue messageQueue : messageQueueCollection) {
            MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
            if (assignedMessageQueueState.get(messageQueue) != null) {
                messageQueueState.setPaused(false);
            }
        }
    }

    public ProcessQueue getProcessQueue(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getProcessQueue();
        }
        return null;
    }

    public long getPullOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getPullOffset();
        }
        return -1;
    }

    public void updatePullOffset(MessageQueue messageQueue, long offset) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            messageQueueState.setPullOffset(offset);
        }
    }

    public long getConsumerOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getConsumeOffset();
        }
        return -1;
    }

    public void updateConsumeOffset(MessageQueue messageQueue, long offset) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            messageQueueState.setConsumeOffset(offset);
        }
    }

    public void setSeekOffset(MessageQueue messageQueue, long offset) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            messageQueueState.setSeekOffset(offset);
        }
    }

    public long getSeekOffset(MessageQueue messageQueue) {
        MessageQueueState messageQueueState = assignedMessageQueueState.get(messageQueue);
        if (messageQueueState != null) {
            return messageQueueState.getSeekOffset();
        }
        return -1;
    }

    public void updateAssignedMessageQueue(String topic, Collection<MessageQueue> assigned) {
        synchronized (this.assignedMessageQueueState) {
            Iterator<Map.Entry<MessageQueue, MessageQueueState>> it = this.assignedMessageQueueState.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<MessageQueue, MessageQueueState> next = it.next();
                if (next.getKey().getTopic().equals(topic)) {
                    //重平衡后需要移除不属于消费者消费的queue 设置为dropped
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

    //region 更新指定消费queue 【移除原指定消费queue中不在新指定queue中的queue、再新增所有新指定的queue】
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

    public void removeAssignedMessageQueue(String topic) {
        synchronized (this.assignedMessageQueueState) {
            this.assignedMessageQueueState.entrySet().removeIf(next -> next.getKey().getTopic().equals(topic));
        }
    }

    public Set<MessageQueue> getAssignedMessageQueues() {
        return this.assignedMessageQueueState.keySet();
    }

    @Data
    private static class MessageQueueState {
        private MessageQueue messageQueue;
        private ProcessQueue processQueue;
        private volatile boolean paused = false;

        /**
         * 下次pull开始位置
         */
        private volatile long pullOffset = -1;
        private volatile long consumeOffset = -1;
        private volatile long seekOffset = -1;

        private MessageQueueState(MessageQueue messageQueue, ProcessQueue processQueue) {
            this.messageQueue = messageQueue;
            this.processQueue = processQueue;
        }
    }
}
