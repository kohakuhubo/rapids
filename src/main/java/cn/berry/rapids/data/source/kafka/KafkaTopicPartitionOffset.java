package cn.berry.rapids.data.source.kafka;

public class KafkaTopicPartitionOffset {

    private volatile boolean complete;

    private final long offset;

    private final int cacheHashCode;

    public KafkaTopicPartitionOffset(long offset) {
        this.offset = offset;
        this.cacheHashCode = Long.hashCode(this.offset);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        KafkaTopicPartitionOffset that = (KafkaTopicPartitionOffset) o;
        return offset == that.offset;
    }

    @Override
    public int hashCode() {
        return this.cacheHashCode;
    }

    public boolean isComplete() {
        return complete;
    }

    public void complete() {
        this.complete = true;
    }

    public long getOffset() {
        return offset;
    }
}
