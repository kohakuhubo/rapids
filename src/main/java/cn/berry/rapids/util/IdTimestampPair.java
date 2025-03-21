package cn.berry.rapids.util;

public class IdTimestampPair {

    private final long id;

    private final long timestamp;

    public IdTimestampPair(long id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    public long getId() {
        return id;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
