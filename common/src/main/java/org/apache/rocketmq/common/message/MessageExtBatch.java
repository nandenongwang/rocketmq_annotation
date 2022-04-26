package org.apache.rocketmq.common.message;

import lombok.Getter;
import lombok.Setter;

import java.nio.ByteBuffer;


public class MessageExtBatch extends MessageExt {

    private static final long serialVersionUID = -2353110995348498537L;

    public ByteBuffer wrap() {
        assert getBody() != null;
        return ByteBuffer.wrap(getBody(), 0, getBody().length);
    }

    /**
     * 批量消息按消息存储协议编码后存储内容
     */
    @Getter
    @Setter
    private ByteBuffer encodedBuff;

}
