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
package org.apache.rocketmq.remoting.protocol;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * RocketMQ 自定义序列化
 */
public class RocketMQSerializable {
    private static final Charset CHARSET_UTF8 = StandardCharsets.UTF_8;

    /**
     * 命令编码
     * 依次写入 命令码+语言码+版本号+请求ID+系统flag+remark长度+remark内容+扩展字段长度+扩展字段内容
     */
    public static byte[] rocketMQProtocolEncode(RemotingCommand cmd) {

        //region 计算 remark length & content
        byte[] remarkBytes = null;
        int remarkLen = 0;
        if (cmd.getRemark() != null && cmd.getRemark().length() > 0) {
            remarkBytes = cmd.getRemark().getBytes(CHARSET_UTF8);
            remarkLen = remarkBytes.length;
        }
        //endregion

        //region 计算extFileds map<string,string> length & content
        byte[] extFieldsBytes = null;
        int extLen = 0;
        if (cmd.getExtFields() != null && !cmd.getExtFields().isEmpty()) {
            extFieldsBytes = mapSerialize(cmd.getExtFields());
            extLen = extFieldsBytes.length;
        }
        //endregion

        //region 计算总长度
        int totalLen = calTotalLen(remarkLen, extLen);
        //endregion

        //region 依次写入命令内容
        ByteBuffer headerBuffer = ByteBuffer.allocate(totalLen);
        // int code(~32767)
        headerBuffer.putShort((short) cmd.getCode());
        // LanguageCode language
        headerBuffer.put(cmd.getLanguage().getCode());
        // int version(~32767)
        headerBuffer.putShort((short) cmd.getVersion());
        // int opaque
        headerBuffer.putInt(cmd.getOpaque());
        // int flag
        headerBuffer.putInt(cmd.getFlag());
        // String remark
        if (remarkBytes != null) {
            headerBuffer.putInt(remarkBytes.length);
            headerBuffer.put(remarkBytes);
        } else {
            headerBuffer.putInt(0);
        }
        // HashMap<String, String> extFields;
        if (extFieldsBytes != null) {
            headerBuffer.putInt(extFieldsBytes.length);
            headerBuffer.put(extFieldsBytes);
        } else {
            headerBuffer.putInt(0);
        }
        //endregion

        return headerBuffer.array();
    }

    /**
     * 序列化map
     * 依次写入 2字节keylength keycontent 4字节valuelength valuecontent
     */
    public static byte[] mapSerialize(HashMap<String, String> map) {
        // keySize+key+valSize+val
        if (null == map || map.isEmpty()) {
            return null;
        }

        int totalLength = 0;
        int kvLength;
        Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey() != null && entry.getValue() != null) {
                kvLength =
                        // keySize + Key
                        2 + entry.getKey().getBytes(CHARSET_UTF8).length
                                // valSize + val
                                + 4 + entry.getValue().getBytes(CHARSET_UTF8).length;
                totalLength += kvLength;
            }
        }

        ByteBuffer content = ByteBuffer.allocate(totalLength);
        byte[] key;
        byte[] val;
        it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            if (entry.getKey() != null && entry.getValue() != null) {
                key = entry.getKey().getBytes(CHARSET_UTF8);
                val = entry.getValue().getBytes(CHARSET_UTF8);

                content.putShort((short) key.length);
                content.put(key);

                content.putInt(val.length);
                content.put(val);
            }
        }

        return content.array();
    }

    /**
     * 计算命令总长度
     * 2字节语言
     */
    private static int calTotalLen(int remark/* remark content length */, int ext/* extFields content length */) {
        return 2//命令码
                + 1//语言码
                + 2//版本
                + 4//请求ID
                + 4//系统flag
                + 4 + remark//remark长度+remark内容
                + 4 + ext;//扩展字段长度+扩展字段内容
    }

    /**
     * 命令解码
     * 按序列化顺序依次解析
     */
    public static RemotingCommand rocketMQProtocolDecode(final byte[] headerArray) {
        RemotingCommand cmd = new RemotingCommand();
        ByteBuffer headerBuffer = ByteBuffer.wrap(headerArray);
        // int code(~32767)
        cmd.setCode(headerBuffer.getShort());
        // LanguageCode language
        cmd.setLanguage(LanguageCode.valueOf(headerBuffer.get()));
        // int version(~32767)
        cmd.setVersion(headerBuffer.getShort());
        // int opaque
        cmd.setOpaque(headerBuffer.getInt());
        // int flag
        cmd.setFlag(headerBuffer.getInt());
        // String remark
        int remarkLength = headerBuffer.getInt();
        if (remarkLength > 0) {
            byte[] remarkContent = new byte[remarkLength];
            headerBuffer.get(remarkContent);
            cmd.setRemark(new String(remarkContent, CHARSET_UTF8));
        }

        // HashMap<String, String> extFields
        int extFieldsLength = headerBuffer.getInt();
        if (extFieldsLength > 0) {
            byte[] extFieldsBytes = new byte[extFieldsLength];
            headerBuffer.get(extFieldsBytes);
            cmd.setExtFields(mapDeserialize(extFieldsBytes));
        }
        return cmd;
    }

    /**
     * 反序列化map
     * 依次读取key&value长度和内容 如；length content length content length content length content
     */
    public static HashMap<String, String> mapDeserialize(byte[] bytes) {
        if (bytes == null || bytes.length <= 0) {
            return null;
        }

        HashMap<String, String> map = new HashMap<>();
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

        short keySize;
        byte[] keyContent;

        int valSize;
        byte[] valContent;

        while (byteBuffer.hasRemaining()) {
            keySize = byteBuffer.getShort();
            keyContent = new byte[keySize];
            byteBuffer.get(keyContent);

            valSize = byteBuffer.getInt();
            valContent = new byte[valSize];
            byteBuffer.get(valContent);

            map.put(new String(keyContent, CHARSET_UTF8), new String(valContent, CHARSET_UTF8));
        }
        return map;
    }

    /**
     * 字符串是否为空
     */
    public static boolean isBlank(String str) {
        int strLen;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
