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

/**
 * 系统响应码
 */
public class RemotingSysResponseCode {

    /**
     * 成功
     */
    public static final int SUCCESS = 0;

    /**
     * 系统错误 如：crc校验失败等
     */
    public static final int SYSTEM_ERROR = 1;

    /**
     * 命令处理队列积压命令过多
     */
    public static final int SYSTEM_BUSY = 2;

    /**
     * 未找到该命令码处理程序
     */
    public static final int REQUEST_CODE_NOT_SUPPORTED = 3;

    /**
     * 事务失败
     */
    public static final int TRANSACTION_FAILED = 4;
}
