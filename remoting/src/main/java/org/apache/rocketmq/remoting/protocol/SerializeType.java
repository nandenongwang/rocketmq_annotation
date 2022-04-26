package org.apache.rocketmq.remoting.protocol;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum SerializeType {
    JSON((byte) 0),
    ROCKETMQ((byte) 1);

    private final byte code;

    /**
     * 获取命令序列化类型
     */
    public static SerializeType valueOf(byte code) {
        for (SerializeType serializeType : SerializeType.values()) {
            if (serializeType.getCode() == code) {
                return serializeType;
            }
        }
        return null;
    }

}
