package org.apache.rocketmq.remoting.common;

import io.netty.channel.Channel;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class RemotingHelper {
    public static final String ROCKETMQ_REMOTING = "RocketmqRemoting";
    public static final String DEFAULT_CHARSET = "UTF-8";

    private static final InternalLogger log = InternalLoggerFactory.getLogger(ROCKETMQ_REMOTING);

    /**
     * 格式化异常消息
     */
    public static String exceptionSimpleDesc(Throwable e) {
        StringBuilder sb = new StringBuilder();
        if (e != null) {
            sb.append(e.toString());

            StackTraceElement[] stackTrace = e.getStackTrace();
            if (stackTrace != null && stackTrace.length > 0) {
                StackTraceElement elment = stackTrace[0];
                sb.append(", ");
                sb.append(elment.toString());
            }
        }

        return sb.toString();
    }

    /**
     * 将如"127.0.0.1:3748"的addr解析成SocketAddress
     */
    public static SocketAddress string2SocketAddress(String addr) {
        int split = addr.lastIndexOf(":");
        String host = addr.substring(0, split);
        String port = addr.substring(split + 1);
        return new InetSocketAddress(host, Integer.parseInt(port));
    }

    /**
     * 发送请求 【同步调用】
     */
    public static RemotingCommand invokeSync(String addr, RemotingCommand request, long timeoutMillis)
            throws InterruptedException, RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException {
        long beginTime = System.currentTimeMillis();
        SocketAddress socketAddress = RemotingUtil.string2SocketAddress(addr);
        SocketChannel socketChannel = RemotingUtil.connect(socketAddress);
        if (socketChannel != null) {
            boolean sendRequestOK = false;

            try {

                socketChannel.configureBlocking(true);

                //region 写出请求
                //bugfix  http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4614802
                socketChannel.socket().setSoTimeout((int) timeoutMillis);

                ByteBuffer byteBufferRequest = request.encode();
                while (byteBufferRequest.hasRemaining()) {
                    int length = socketChannel.write(byteBufferRequest);
                    if (length > 0) {
                        if (byteBufferRequest.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {

                                throw new RemotingSendRequestException(addr);
                            }
                        }
                    } else {
                        throw new RemotingSendRequestException(addr);
                    }

                    Thread.sleep(1);
                }

                //endregion

                sendRequestOK = true;

                //region 读取响应长度
                ByteBuffer byteBufferSize = ByteBuffer.allocate(4);
                while (byteBufferSize.hasRemaining()) {
                    int length = socketChannel.read(byteBufferSize);
                    if (length > 0) {
                        if (byteBufferSize.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {

                                throw new RemotingTimeoutException(addr, timeoutMillis);
                            }
                        }
                    } else {
                        throw new RemotingTimeoutException(addr, timeoutMillis);
                    }

                    Thread.sleep(1);
                }
                //endregion

                //region 读取响应
                int size = byteBufferSize.getInt(0);
                ByteBuffer byteBufferBody = ByteBuffer.allocate(size);
                while (byteBufferBody.hasRemaining()) {
                    int length = socketChannel.read(byteBufferBody);
                    if (length > 0) {
                        if (byteBufferBody.hasRemaining()) {
                            if ((System.currentTimeMillis() - beginTime) > timeoutMillis) {

                                throw new RemotingTimeoutException(addr, timeoutMillis);
                            }
                        }
                    } else {
                        throw new RemotingTimeoutException(addr, timeoutMillis);
                    }

                    Thread.sleep(1);
                }
                byteBufferBody.flip();
                //endregion

                //region 解析返回命令响应
                return RemotingCommand.decode(byteBufferBody);
                //endregion
            } catch (IOException e) {
                log.error("invokeSync failure", e);

                if (sendRequestOK) {
                    throw new RemotingTimeoutException(addr, timeoutMillis);
                } else {
                    throw new RemotingSendRequestException(addr);
                }
            } finally {
                try {
                    socketChannel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            throw new RemotingConnectException(addr);
        }
    }

    /**
     * 获取channel远程地址
     */
    public static String parseChannelRemoteAddr(final Channel channel) {
        if (null == channel) {
            return "";
        }
        SocketAddress remote = channel.remoteAddress();
        final String addr = remote != null ? remote.toString() : "";

        if (addr.length() > 0) {
            int index = addr.lastIndexOf("/");
            if (index >= 0) {
                return addr.substring(index + 1);
            }

            return addr;
        }

        return "";
    }

    /**
     * 解析SocketAddress地址
     */
    public static String parseSocketAddressAddr(SocketAddress socketAddress) {
        if (socketAddress != null) {
            final String addr = socketAddress.toString();

            if (addr.length() > 0) {
                return addr.substring(1);
            }
        }
        return "";
    }
}
