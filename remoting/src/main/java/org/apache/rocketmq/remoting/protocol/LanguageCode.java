package org.apache.rocketmq.remoting.protocol;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * 客户端语言编码
 */
@Getter
@AllArgsConstructor
public enum LanguageCode {
    JAVA((byte) 0),
    CPP((byte) 1),
    DOTNET((byte) 2),
    PYTHON((byte) 3),
    DELPHI((byte) 4),
    ERLANG((byte) 5),
    RUBY((byte) 6),
    OTHER((byte) 7),
    HTTP((byte) 8),
    GO((byte) 9),
    PHP((byte) 10),
    OMS((byte) 11);

    private final byte code;

    /**
     * 获取语言类型
     */
    public static LanguageCode valueOf(byte code) {
        for (LanguageCode languageCode : LanguageCode.values()) {
            if (languageCode.getCode() == code) {
                return languageCode;
            }
        }
        return null;
    }

}
