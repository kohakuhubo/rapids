package cn.berry.rapids.util;

public class SnowflakeIdWorker {

    private final static long workerIdBits = 10L;

    private final static long clockBits = 2L;

    private final static long sequenceBits = 10L;

    private final static long maxWorkerId = ~(-1L << workerIdBits);

    private final static long maxClockId = ~(-1L << clockBits);

    private final static long maxSequence = ~(-1L << sequenceBits);

    private final static long clockIdShift = sequenceBits;

    private final static long workerIdShift = sequenceBits + clockBits;

    private final static long timestampLeftShift = sequenceBits + clockBits + workerIdBits;

    private final long workerId;

    private long clockId;

    private long sequence = 0L;

    private long lastTimestamp = -1L;

    public SnowflakeIdWorker(long workerId) {
        if (workerId > maxWorkerId || workerId < 0) {
            throw new IllegalArgumentException();
        }
        this.workerId = workerId;
    }

    public SnowflakeIdWorker(long workerId, long clockId, long sequence, long lastTimestamp) {
        this(workerId);
        this.clockId = clockId;
        this.sequence = sequence;
        this.lastTimestamp = lastTimestamp;
    }

    public static SnowflakeIdWorker getInstance(long id) {
        return new SnowflakeIdWorker(SnowflakeIdWorker.getWorkId(id),
                SnowflakeIdWorker.getLastTimestamp(id),
                SnowflakeIdWorker.getClockId(id),
                SnowflakeIdWorker.getSequence(id));
    }

    public synchronized IdTimestampPair nextId() {
        long timestamp = System.currentTimeMillis();
        if (timestamp < lastTimestamp) {
            clockId = (clockId + 1) & maxClockId;
        } else if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & maxSequence;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0;
        }
        lastTimestamp = timestamp;
        long id = (timestamp << timestampLeftShift)
                | (workerId << workerIdShift)
                | (clockId << clockIdShift)
                | sequence;
        return new IdTimestampPair(id, timestamp);
    }

    public synchronized void batchWriteIds(int n, Consumer consumer) {
        long left = n;
        long idTmp = nextId().getId();
        while (left > 0) {
            long range = maxSequence - sequence + 1;
            if (left <= range) {
                for (int i = 0; i < left; i++) {
                    consumer.accept(idTmp++);
                }
                sequence = sequence + 1;
                return;
            } else {
                for (int i = 0; i < range; i++) {
                    consumer.accept(idTmp++);
                }
                sequence = 0;
                left = left - range;
                long timestamp = tilNextMillis(lastTimestamp);
                lastTimestamp = timestamp;
                idTmp = (timestamp << timestampLeftShift)
                        | (workerId << workerIdShift)
                        | (clockId << clockIdShift)
                        | sequence;
            }
        }
    }

    public static long getLastTimestamp(long id) {
        return id >> timestampLeftShift;
    }

    public static long getWorkId(long id) {
        return (id >> timestampLeftShift) & maxWorkerId;
    }

    public static long getClockId(long id) {
        return (id >> clockIdShift) & maxClockId;
    }

    public static long getSequence(long id) {
        return id & maxSequence;
    }

    private long tilNextMillis(long lastTimestamp) {
        try {
            int retryTimes = 3;
            while (true) {
                Thread.sleep(1L);
                long now = System.currentTimeMillis();
                if (now > lastTimestamp) {
                    return now;
                }
                retryTimes--;
                if (retryTimes == 0) {
                    clockId = (clockId + 1) & maxClockId;
                    return now;
                }
            }
        } catch (InterruptedException e) {
            //ignore
        }
        return lastTimestamp + 1;
    }

    public interface Consumer {
        void accept(long id);
    }

}
