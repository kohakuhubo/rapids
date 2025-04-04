package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.Pair;
import cn.berry.rapids.data.source.SourceEntry;

import java.util.List;

/**
 * Kafka数据源条目类
 * 
 * 描述: 封装Kafka数据源的条目信息。
 * 
 * 特性:
 * 1. 存储主题名称
 * 2. 存储分区号
 * 3. 存储偏移量
 * 
 * @author Berry
 * @version 1.0.0
 */
public class KafkaSourceEntry implements SourceEntry<KafkaSourceEntry> {

    private final List<KafkaTopicRecords> kafkaTopicRecordsList;

    private final KafkaSource kafkaSource;

    private List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> commitOffset;

    public KafkaSourceEntry(KafkaSource kafkaSource) {
        this.kafkaSource = kafkaSource;
        this.kafkaTopicRecordsList = null;
    }

    /**
     * 构造Kafka数据源条目
     *
     * @param kafkaSource Kafka数据源
     * @param kafkaTopicRecordsList 消费者记录
     */
    public KafkaSourceEntry(KafkaSource kafkaSource, List<KafkaTopicRecords> kafkaTopicRecordsList) {
        this.kafkaSource = kafkaSource;
        this.kafkaTopicRecordsList = kafkaTopicRecordsList;
        this.commitOffset = null;
    }

    /**
     * 获取ID
     * 
     * @return ID
     */
    @Override
    public long id() {
        return 0;
    }

    /**
     * 获取条目
     * 
     * @return 条目
     */
    @Override
    public KafkaSourceEntry entry() {
        return this;
    }

    /**
     * 成功
     */
    @Override
    public void success() {
        this.kafkaSource.commit(this);
    }

    /**
     * 失败
     */
    @Override
    public void fail() {

    }

    /**
     * 获取Kafka数据源
     * 
     * @return Kafka数据源
     */
    public KafkaSource getKafkaSource() {
        return kafkaSource;
    }

    public List<KafkaTopicRecords> getKafkaTopicRecordsList() {
        return kafkaTopicRecordsList;
    }

    public List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> getCommitOffset() {
        return commitOffset;
    }

    public void setCommitOffset(List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> commitOffset) {
        this.commitOffset = commitOffset;
    }
}
