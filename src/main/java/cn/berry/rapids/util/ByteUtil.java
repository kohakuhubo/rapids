package cn.berry.rapids.util;

public class ByteUtil {

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
