package org.apache.rocketmq.remoting;

import org.apache.rocketmq.remoting.exception.RemotingCommandException;

public interface CommandCustomHeader {
    /**
     * 检查自定义命令header的值是否符合格式
     */
    void checkFields() throws RemotingCommandException;
}
