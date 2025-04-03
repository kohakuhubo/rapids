package cn.berry.rapids.util;

/**
 * 字节工具类
 * 
 * 描述: 提供字节数组操作的工具方法，用于处理字节数据的转换和比较。
 * 
 * 特性:
 * 1. 支持字节数组比较
 * 2. 提供字节数组哈希计算
 * 3. 支持字节数组转字符串
 * 
 * @author Berry
 * @version 1.0.0
 */
public class ByteUtil {

    /**
     * 比较两个字节数组是否相等
     * 
     * @param a 第一个字节数组
     * @param b 第二个字节数组
     * @return 如果两个字节数组相等则返回true，否则返回false
     */
    public static boolean equals(byte[] a, byte[] b) {
        if (a == b) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * 计算字节数组的哈希值
     * 
     * @param bytes 字节数组
     * @return 哈希值
     */
    public static int hashCode(byte[] bytes) {
        if (bytes == null) {
            return 0;
        }
        int result = 1;
        for (byte b : bytes) {
            result = 31 * result + b;
        }
        return result;
    }

    /**
     * 将字节数组转换为字符串
     * 
     * @param bytes 字节数组
     * @return 字符串表示
     */
    public static String toString(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < bytes.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(bytes[i]);
        }
        sb.append(']');
        return sb.toString();
    }

    public static byte readByte(byte[] bytes, int offset) {
        return bytes[offset];
    }

    public static int readInt16be(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 8) | (bytes[offset + 1] & 0xFF);
    }

    public static int readInt32be(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24)
                | ((bytes[offset + 1] & 0xFF) << 16)
                | ((bytes[offset + 2] & 0xFF) << 8)
                | (bytes[offset + 3] & 0xFF);
    }

    public static long readInt64be(byte[] bytes, int offset) {
        return (((long) bytes[offset] & 0xFF) << 56)
                | (((long) bytes[offset + 1] & 0xFF) << 48)
                | (((long) bytes[offset + 2] & 0xFF) << 40)
                | (((long) bytes[offset + 3] & 0xFF) << 32)
                | (((long) bytes[offset + 4] & 0xFF) << 24)
                | (((long) bytes[offset + 5] & 0xFF) << 16)
                | (((long) bytes[offset + 6] & 0xFF) << 8)
                | ((long) bytes[offset + 7] & 0xFF);
    }

    public static int readInt16le(byte[] bytes, int offset) {
        return ((bytes[offset + 1] & 0xFF) << 8) | (bytes[offset] & 0xFF);
    }

    public static int readInt32le(byte[] bytes, int offset) {
        return ((bytes[offset + 3] & 0xFF) << 24)
                | ((bytes[offset + 2] & 0xFF) << 16)
                | ((bytes[offset + 1] & 0xFF) << 8)
                | (bytes[offset] & 0xFF);
    }

    public static long readInt64le(byte[] bytes, int offset) {
        return (((long) bytes[offset + 7] & 0xFF) << 56)
                | (((long) bytes[offset + 6] & 0xFF) << 48)
                | (((long) bytes[offset + 5] & 0xFF) << 40)
                | (((long) bytes[offset + 4] & 0xFF) << 32)
                | (((long) bytes[offset + 3] & 0xFF) << 24)
                | (((long) bytes[offset + 2] & 0xFF) << 16)
                | (((long) bytes[offset + 1] & 0xFF) << 8)
                | ((long) bytes[offset] & 0xFF);
    }
}
