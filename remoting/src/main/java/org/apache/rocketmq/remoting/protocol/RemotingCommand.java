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

import com.alibaba.fastjson.annotation.JSONField;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.annotation.CFNotNull;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;

@NoArgsConstructor
public class RemotingCommand {
    private static InternalLogger log = InternalLoggerFactory.getLogger(RemotingHelper.ROCKETMQ_REMOTING);

    //region 扩展字段值类型规范名 【支持】
    private static final String STRING_CANONICAL_NAME = String.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_1 = Double.class.getCanonicalName();
    private static final String DOUBLE_CANONICAL_NAME_2 = double.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_1 = Integer.class.getCanonicalName();
    private static final String INTEGER_CANONICAL_NAME_2 = int.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_1 = Long.class.getCanonicalName();
    private static final String LONG_CANONICAL_NAME_2 = long.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_1 = Boolean.class.getCanonicalName();
    private static final String BOOLEAN_CANONICAL_NAME_2 = boolean.class.getCanonicalName();
    //endregion


    /**
     * 请求ID源
     */
    private static final AtomicInteger requestId = new AtomicInteger(0);

    /**
     * 默认序列化类型
     */
    @Getter
    private static SerializeType serializeTypeConfigInThisServer = SerializeType.JSON;

    //region 从系统属性与环境变量中获取序列化类型 默认使用json序列化
    public static String SERIALIZE_TYPE_PROPERTY = "rocketmq.serialize.type";
    public static String SERIALIZE_TYPE_ENV = "ROCKETMQ_SERIALIZE_TYPE";

    static {
        String protocol = System.getProperty(SERIALIZE_TYPE_PROPERTY, System.getenv(SERIALIZE_TYPE_ENV));
        if (!isBlank(protocol)) {
            try {
                serializeTypeConfigInThisServer = SerializeType.valueOf(protocol);
            } catch (IllegalArgumentException e) {
                throw new RuntimeException("parser specified protocol error. protocol=" + protocol, e);
            }
        }
    }
    //endregion

    /**
     * 命令码
     */
    @Getter
    @Setter
    private int code;

    /**
     * 语言类型
     */
    @Getter
    @Setter
    private LanguageCode language = LanguageCode.JAVA;

    /**
     * 版本号
     */
    @Getter
    @Setter
    private int version = 0;

    /**
     * 请求ID
     */
    @Getter
    @Setter
    private int opaque = requestId.getAndIncrement();

    /**
     * 系统flag
     */
    @Getter
    @Setter
    private int flag = 0;

    /**
     * remark
     */
    @Getter
    @Setter
    private String remark;

    /**
     * 扩展字段 存放自定义命令头
     */
    @Getter
    @Setter
    private HashMap<String, String> extFields;

    /**
     * 自定义命令头
     */
    private transient CommandCustomHeader customHeader;

    /**
     * 序列化类型
     */
    @Getter
    @Setter
    private SerializeType serializeTypeCurrentRPC = serializeTypeConfigInThisServer;

    /**
     * 命令体
     */
    @Getter
    @Setter
    private transient byte[] body;

    /**
     * 根据命令码和自定义请求头创建命令
     */
    public static RemotingCommand createRequestCommand(int code, CommandCustomHeader customHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setCode(code);
        cmd.customHeader = customHeader;
        setCmdVersion(cmd);
        return cmd;
    }

    //region 设置命令版本号 默认从系统属性取值
    public static String REMOTING_VERSION_KEY = "rocketmq.remoting.version";
    private static volatile int configVersion = -1;

    private static void setCmdVersion(RemotingCommand cmd) {
        if (configVersion >= 0) {
            cmd.setVersion(configVersion);
        } else {
            String v = System.getProperty(REMOTING_VERSION_KEY);
            if (v != null) {
                int value = Integer.parseInt(v);
                cmd.setVersion(value);
                configVersion = value;
            }
        }
    }
    //endregion

    /**
     * 创建错误响应命令
     */
    public static RemotingCommand createResponseCommand(Class<? extends CommandCustomHeader> classHeader) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR, "not set any response code", classHeader);
    }

    /**
     * 创建空响应命令
     */
    public static RemotingCommand createResponseCommand(int code, String remark, Class<? extends CommandCustomHeader> classHeader) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.markResponseType();
        cmd.setCode(code);
        cmd.setRemark(remark);
        setCmdVersion(cmd);

        if (classHeader != null) {
            try {
                cmd.customHeader = classHeader.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                return null;
            }
        }

        return cmd;
    }

    public static RemotingCommand createResponseCommand(int code, String remark) {
        return createResponseCommand(code, remark, null);
    }

    //region 命令解码 按编码逆序解码
    public static RemotingCommand decode(byte[] array) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(array);
        return decode(byteBuffer);
    }

    public static RemotingCommand decode(ByteBuffer byteBuffer) {
        int length = byteBuffer.limit();
        int oriHeaderLen = byteBuffer.getInt();
        int headerLength = getHeaderLength(oriHeaderLen);

        byte[] headerData = new byte[headerLength];
        byteBuffer.get(headerData);

        RemotingCommand cmd = headerDecode(headerData, getProtocolType(oriHeaderLen));

        int bodyLength = length - 4 - headerLength;
        byte[] bodyData = null;
        if (bodyLength > 0) {
            bodyData = new byte[bodyLength];
            byteBuffer.get(bodyData);
        }
        cmd.body = bodyData;

        return cmd;
    }

    public static int getHeaderLength(int length) {
        return length & 0xFFFFFF;
    }

    private static RemotingCommand headerDecode(byte[] headerData, SerializeType type) {
        switch (type) {
            case JSON:
                RemotingCommand resultJson = RemotingSerializable.decode(headerData, RemotingCommand.class);
                resultJson.setSerializeTypeCurrentRPC(type);
                return resultJson;
            case ROCKETMQ:
                RemotingCommand resultRMQ = RocketMQSerializable.rocketMQProtocolDecode(headerData);
                resultRMQ.setSerializeTypeCurrentRPC(type);
                return resultRMQ;
            default:
                break;
        }

        return null;
    }

    /**
     * 解码自定义命令头 反射
     */
    public CommandCustomHeader decodeCommandCustomHeader(Class<? extends CommandCustomHeader> classHeader) throws RemotingCommandException {
        CommandCustomHeader objectHeader;
        try {
            objectHeader = classHeader.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            return null;
        }

        if (this.extFields != null) {
            Field[] fields = getClazzFields(classHeader);
            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String fieldName = field.getName();
                    if (!fieldName.startsWith("this")) {
                        try {
                            String value = this.extFields.get(fieldName);
                            if (null == value) {
                                if (!isFieldNullable(field)) {
                                    throw new RemotingCommandException("the custom field <" + fieldName + "> is null");
                                }
                                continue;
                            }

                            field.setAccessible(true);
                            String type = getCanonicalName(field.getType());
                            Object valueParsed;

                            if (type.equals(STRING_CANONICAL_NAME)) {
                                valueParsed = value;
                            } else if (type.equals(INTEGER_CANONICAL_NAME_1) || type.equals(INTEGER_CANONICAL_NAME_2)) {
                                valueParsed = Integer.parseInt(value);
                            } else if (type.equals(LONG_CANONICAL_NAME_1) || type.equals(LONG_CANONICAL_NAME_2)) {
                                valueParsed = Long.parseLong(value);
                            } else if (type.equals(BOOLEAN_CANONICAL_NAME_1) || type.equals(BOOLEAN_CANONICAL_NAME_2)) {
                                valueParsed = Boolean.parseBoolean(value);
                            } else if (type.equals(DOUBLE_CANONICAL_NAME_1) || type.equals(DOUBLE_CANONICAL_NAME_2)) {
                                valueParsed = Double.parseDouble(value);
                            } else {
                                throw new RemotingCommandException("the custom field <" + fieldName + "> type is not supported");
                            }

                            field.set(objectHeader, valueParsed);

                        } catch (Throwable e) {
                            log.error("Failed field [{}] decoding", fieldName, e);
                        }
                    }
                }
            }

            objectHeader.checkFields();
        }

        return objectHeader;
    }
    //endregion

    /**
     * 创建请求ID 依次递增
     */
    public static int createNewRequestId() {
        return requestId.getAndIncrement();
    }

    /**
     * 判断字符串是否为空
     */
    private static boolean isBlank(String str) {
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

    //region 协议类型编解码 4字节 1字节序列化类型code + 3字节头内容大小

    public static SerializeType getProtocolType(int source/* 头内容大小*/) {
        return SerializeType.valueOf((byte) ((source >> 24) & 0xFF));
    }

    public static byte[] markProtocolType(int source, SerializeType type) {
        byte[] result = new byte[4];

        result[0] = type.getCode();//序列化协议码 0-json 1-rocketmq
        result[1] = (byte) ((source >> 16) & 0xFF);
        result[2] = (byte) ((source >> 8) & 0xFF);
        result[3] = (byte) (source & 0xFF);
        return result;
    }
    //endregion

    public CommandCustomHeader readCustomHeader() {
        return customHeader;
    }

    public void writeCustomHeader(CommandCustomHeader customHeader) {
        this.customHeader = customHeader;
    }


    //region 反射获取自定义命令头所有fields
    /**
     * 缓存自定义命令头所有fields 【避免频繁反射】
     */
    private static Map<Class<? extends CommandCustomHeader>, Field[]> CLASS_HASH_MAP = new HashMap<>();

    private Field[] getClazzFields(Class<? extends CommandCustomHeader> classHeader) {
        Field[] field = CLASS_HASH_MAP.get(classHeader);

        if (field == null) {
            field = classHeader.getDeclaredFields();
            synchronized (CLASS_HASH_MAP) {
                CLASS_HASH_MAP.put(classHeader, field);
            }
        }
        return field;
    }
    //endregion

    //region 判断field是否有@CFNotNull注解
    private static Map<Field, Boolean> NULLABLE_FIELD_CACHE = new HashMap<>();

    private boolean isFieldNullable(Field field) {
        if (!NULLABLE_FIELD_CACHE.containsKey(field)) {
            Annotation annotation = field.getAnnotation(CFNotNull.class);
            synchronized (NULLABLE_FIELD_CACHE) {
                NULLABLE_FIELD_CACHE.put(field, annotation == null);
            }
        }
        return NULLABLE_FIELD_CACHE.get(field);
    }
    //endregion

    //region 获取类的规范名
    private static Map<Class, String> CANONICAL_NAME_CACHE = new HashMap<>();

    private String getCanonicalName(Class clazz) {
        String name = CANONICAL_NAME_CACHE.get(clazz);

        if (name == null) {
            name = clazz.getCanonicalName();
            synchronized (CANONICAL_NAME_CACHE) {
                CANONICAL_NAME_CACHE.put(clazz, name);
            }
        }
        return name;
    }
    //endregion

    //region 命令编码 【输出内容 4字节包总大小 +  4字节序列化类型&头内容大小 + 头内容 + 体内容】
    public ByteBuffer encode() {
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData = this.headerEncode();
        length += headerData.length;

        // 3> body data length
        if (this.body != null) {
            length += body.length;
        }

        ByteBuffer result = ByteBuffer.allocate(4 + length);

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        // body data;
        if (this.body != null) {
            result.put(this.body);
        }

        result.flip();

        return result;
    }

    /**
     * 命令头编码
     */
    private byte[] headerEncode() {
        this.makeCustomHeaderToNet();
        if (SerializeType.ROCKETMQ == serializeTypeCurrentRPC) {
            return RocketMQSerializable.rocketMQProtocolEncode(this);
        } else {
            return RemotingSerializable.encode(this);
        }
    }

    /**
     * 将自定义头对象各field取出置于extFields map中
     */
    public void makeCustomHeaderToNet() {
        if (this.customHeader != null) {
            Field[] fields = getClazzFields(customHeader.getClass());
            if (null == this.extFields) {
                this.extFields = new HashMap<>();
            }

            for (Field field : fields) {
                if (!Modifier.isStatic(field.getModifiers())) {
                    String name = field.getName();
                    if (!name.startsWith("this")) {
                        Object value = null;
                        try {
                            field.setAccessible(true);
                            value = field.get(this.customHeader);
                        } catch (Exception e) {
                            log.error("Failed to access field [{}]", name, e);
                        }

                        if (value != null) {
                            this.extFields.put(name, value.toString());
                        }
                    }
                }
            }
        }
    }

    //region  header编码 【输出内容 4字节包总大小 +  4字节序列化类型&头内容大小 + 头内容】
    public ByteBuffer encodeHeader() {
        return encodeHeader(this.body != null ? this.body.length : 0);
    }

    public ByteBuffer encodeHeader(int bodyLength) {
        //region 计算 header长度
        // 1> header length size
        int length = 4;

        // 2> header data length
        byte[] headerData;
        headerData = this.headerEncode();

        length += headerData.length;

        // 3> body data length
        length += bodyLength;
        //endregion

        ByteBuffer result = ByteBuffer.allocate(4/* 头大小 */ + length/* 4字节协议类型 + n字节头内容 */ - bodyLength);

        //region 依次写入

        // length
        result.putInt(length);

        // header length
        result.put(markProtocolType(headerData.length, serializeTypeCurrentRPC));

        // header data
        result.put(headerData);

        result.flip();
        //endregion


        return result;
    }
    //endregion

    //endregion

    //region 设置 & 判断 是否是oneway命令 否则为RPC命令【系统flag第2bit位是否为1】
    private static int RPC_ONEWAY = 1; // 0, RPC

    public void markOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isOnewayRPC() {
        int bits = 1 << RPC_ONEWAY;
        return (this.flag & bits) == bits;
    }
    //endregion

    //region 设置 & 判断是否是响应命令 否则是请求命令 【系统flag第1bit位是否为1】
    private static int RPC_TYPE = 0; // 0, REQUEST_COMMAND

    public void markResponseType() {
        int bits = 1 << RPC_TYPE;
        this.flag |= bits;
    }

    @JSONField(serialize = false)
    public boolean isResponseType() {
        int bits = 1 << RPC_TYPE;
        return (this.flag & bits) == bits;
    }
    //endregion

    /**
     * 获取命令类型 请求或响应
     */
    @JSONField(serialize = false)
    public RemotingCommandType getType() {
        if (this.isResponseType()) {
            return RemotingCommandType.RESPONSE_COMMAND;
        }

        return RemotingCommandType.REQUEST_COMMAND;
    }

    /**
     * 增加扩展字段
     */
    public void addExtField(String key, String value) {
        if (null == extFields) {
            extFields = new HashMap<>();
        }
        extFields.put(key, value);
    }

    @Override
    public String toString() {
        return "RemotingCommand [code=" + code + ", language=" + language + ", version=" + version + ", opaque=" + opaque + ", flag(B)="
                + Integer.toBinaryString(flag) + ", remark=" + remark + ", extFields=" + extFields + ", serializeTypeCurrentRPC="
                + serializeTypeCurrentRPC + "]";
    }

}