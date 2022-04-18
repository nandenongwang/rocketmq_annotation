package org.apache.rocketmq.common.protocol.header.namesrv;

import lombok.Data;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;

@Data
public class RegisterBrokerRequestHeader implements CommandCustomHeader {
    @CFNotNull
    private String brokerName;
    @CFNotNull
    private String brokerAddr;
    @CFNotNull
    private String clusterName;
    @CFNotNull
    private String haServerAddr;
    @CFNotNull
    private Long brokerId;

    private boolean compressed;

    private Integer bodyCrc32 = 0;

    @Override
    public void checkFields() throws RemotingCommandException {
    }

}
