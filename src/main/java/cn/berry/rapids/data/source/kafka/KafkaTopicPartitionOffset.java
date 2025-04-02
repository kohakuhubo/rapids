package cn.berry.rapids.data.source.kafka;

/**
 * Kafka主题分区偏移量类
 * 
 * 描述: 封装Kafka主题分区的偏移量信息。
 * 
 * 特性:
 * 1. 存储主题名称
 * 2. 存储分区号
 * 3. 存储偏移量
 * 
 * @author Berry
 * @version 1.0.0
 */
public class KafkaTopicPartitionOffset {

    private volatile boolean complete;

    private final long offset;

    private final int cacheHashCode;

    /**
     * 构造Kafka主题分区偏移量
     * 
     * @param offset 偏移量
     */
    public KafkaTopicPartitionOffset(long offset) {
        this.offset = offset;
        this.cacheHashCode = Long.hashCode(this.offset);
    }

    /**
     * 检查是否完成
     * 
     * @return 是否完成
     */
    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        KafkaTopicPartitionOffset that = (KafkaTopicPartitionOffset) o;
        return offset == that.offset;
    }

    /**
     * 获取哈希码
     * 
     * @return 哈希码
     */
    @Override
    public int hashCode() {
        return this.cacheHashCode;
    }

    /**
     * 检查是否完成
     * 
     * @return 是否完成
     */
    public boolean isComplete() {
        return complete;
    }

    /**
     * 完成
     */
    public void complete() {
        this.complete = true;
    }

    /**
     * 获取偏移量
     * 
     * @return 偏移量
     */
    public long getOffset() {
        return offset;
    }
}
