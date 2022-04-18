/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.message;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;

import lombok.*;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
@Data
public class MessageExt extends Message {
    private static final long serialVersionUID = 5720810158625748049L;

    private String brokerName;

    private String msgId;

    private long commitLogOffset;

    private int queueId;

    private long queueOffset;

    private int storeSize;

    private long storeTimestamp;

    private SocketAddress storeHost;

    private SocketAddress bornHost;

    private long bornTimestamp;

    private int bodyCRC;

    private int sysFlag;

    private int reconsumeTimes;

    private long preparedTransactionOffset;

    public MessageExt(int queueId, long bornTimestamp, SocketAddress bornHost, long storeTimestamp, SocketAddress storeHost, String msgId) {
        this.queueId = queueId;
        this.bornTimestamp = bornTimestamp;
        this.bornHost = bornHost;
        this.storeTimestamp = storeTimestamp;
        this.storeHost = storeHost;
        this.msgId = msgId;
    }

    public static TopicFilterType parseTopicFilterType(final int sysFlag) {
        if ((sysFlag & MessageSysFlag.MULTI_TAGS_FLAG) == MessageSysFlag.MULTI_TAGS_FLAG) {
            return TopicFilterType.MULTI_TAG;
        }

        return TopicFilterType.SINGLE_TAG;
    }

    public static ByteBuffer socketAddress2ByteBuffer(final SocketAddress socketAddress, final ByteBuffer byteBuffer) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        InetAddress address = inetSocketAddress.getAddress();
        if (address instanceof Inet4Address) {
            byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 4);
        } else {
            byteBuffer.put(inetSocketAddress.getAddress().getAddress(), 0, 16);
        }
        byteBuffer.putInt(inetSocketAddress.getPort());
        byteBuffer.flip();
        return byteBuffer;
    }

    public static ByteBuffer socketAddress2ByteBuffer(SocketAddress socketAddress) {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
        InetAddress address = inetSocketAddress.getAddress();
        ByteBuffer byteBuffer;
        if (address instanceof Inet4Address) {
            byteBuffer = ByteBuffer.allocate(4 + 4);
        } else {
            byteBuffer = ByteBuffer.allocate(16 + 4);
        }
        return socketAddress2ByteBuffer(socketAddress, byteBuffer);
    }

    public ByteBuffer getBornHostBytes() {
        return socketAddress2ByteBuffer(this.bornHost);
    }

    public ByteBuffer getBornHostBytes(ByteBuffer byteBuffer) {
        return socketAddress2ByteBuffer(this.bornHost, byteBuffer);
    }

    public ByteBuffer getStoreHostBytes() {
        return socketAddress2ByteBuffer(this.storeHost);
    }

    public ByteBuffer getStoreHostBytes(ByteBuffer byteBuffer) {
        return socketAddress2ByteBuffer(this.storeHost, byteBuffer);
    }

    public String getBornHostString() {
        if (null != this.bornHost) {
            InetAddress inetAddress = ((InetSocketAddress) this.bornHost).getAddress();

            return null != inetAddress ? inetAddress.getHostAddress() : null;
        }

        return null;
    }

    public String getBornHostNameString() {
        if (null != this.bornHost) {
            InetAddress inetAddress = ((InetSocketAddress) this.bornHost).getAddress();

            return null != inetAddress ? inetAddress.getHostName() : null;
        }

        return null;
    }

    public void setStoreHostAddressV6Flag() {
        this.sysFlag = this.sysFlag | MessageSysFlag.STOREHOSTADDRESS_V6_FLAG;
    }

    public void setBornHostV6Flag() {
        this.sysFlag = this.sysFlag | MessageSysFlag.BORNHOST_V6_FLAG;
    }

}
