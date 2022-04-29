package org.apache.rocketmq.common.utils;

import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * 轻量IO工具类
 */
public class IOTinyUtils {

    /**
     * 读取InputStream内容
     */
    static public String toString(InputStream input, String encoding) throws IOException {
        return (null == encoding) ? toString(new InputStreamReader(input, RemotingHelper.DEFAULT_CHARSET)) : toString(new InputStreamReader(input, encoding));
    }

    /**
     * 读取Reader内容
     */
    static public String toString(Reader reader) throws IOException {
        CharArrayWriter sw = new CharArrayWriter();
        copy(reader, sw);
        return sw.toString();
    }

    /**
     * 复制Reader写出到Writer
     */
    static public long copy(Reader input, Writer output) throws IOException {
        char[] buffer = new char[1 << 12];
        long count = 0;
        for (int n = 0; (n = input.read(buffer)) >= 0; ) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

    /**
     * 按行读取Reader内容
     */
    static public List<String> readLines(Reader input) throws IOException {
        BufferedReader reader = toBufferedReader(input);
        List<String> list = new ArrayList<>();
        String line;
        for (; ; ) {
            line = reader.readLine();
            if (null != line) {
                list.add(line);
            } else {
                break;
            }
        }
        return list;
    }

    /**
     * 包装Reader成BufferedReader
     */
    static private BufferedReader toBufferedReader(Reader reader) {
        return reader instanceof BufferedReader ? (BufferedReader) reader : new BufferedReader(reader);
    }

    /**
     * 拷贝文件内容
     */
    static public void copyFile(String source, String target) throws IOException {
        File sf = new File(source);
        if (!sf.exists()) {
            throw new IllegalArgumentException("source file does not exist.");
        }
        File tf = new File(target);
        tf.getParentFile().mkdirs();
        if (!tf.exists() && !tf.createNewFile()) {
            throw new RuntimeException("failed to create target file.");
        }

        try (FileChannel tc = new FileOutputStream(tf).getChannel(); FileChannel sc = new FileInputStream(sf).getChannel()) {
            sc.transferTo(0, sc.size(), tc);
        }
    }

    /**
     * 删除文件或目录
     */
    public static void delete(File fileOrDir) throws IOException {
        if (fileOrDir == null) {
            return;
        }

        if (fileOrDir.isDirectory()) {
            cleanDirectory(fileOrDir);
        }

        fileOrDir.delete();
    }

    /**
     * 删除目录下所有文件
     */
    public static void cleanDirectory(File directory) throws IOException {
        if (!directory.exists()) {
            String message = directory + " does not exist";
            throw new IllegalArgumentException(message);
        }

        if (!directory.isDirectory()) {
            String message = directory + " is not a directory";
            throw new IllegalArgumentException(message);
        }

        File[] files = directory.listFiles();
        if (files == null) { // null if security restricted
            throw new IOException("Failed to list contents of " + directory);
        }

        IOException exception = null;
        for (File file : files) {
            try {
                delete(file);
            } catch (IOException ioe) {
                exception = ioe;
            }
        }

        if (null != exception) {
            throw exception;
        }
    }

    /**
     * 写出文本到文件
     */
    public static void writeStringToFile(File file, String data, String encoding) throws IOException {
        try (OutputStream os = new FileOutputStream(file)) {
            os.write(data.getBytes(encoding));
        }
    }
}
