package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.Pair;
import cn.berry.rapids.data.source.SourceEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;

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

    private final List<ConsumerRecord<String, byte[]>> records;

    private final KafkaSource kafkaSource;

    private final List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> ktpOffsets;

    /**
     * 构造Kafka数据源条目
     * 
     * @param records 消费者记录
     * @param kafkaSource Kafka数据源
     * @param ktpOffsets 主题分区偏移量
     */
    public KafkaSourceEntry(List<ConsumerRecord<String, byte[]>> records, KafkaSource kafkaSource, List<Pair<KafkaTopicPartition,
            List<KafkaTopicPartitionOffset>>> ktpOffsets) {
        this.records = records;
        this.kafkaSource = kafkaSource;
        this.ktpOffsets = ktpOffsets;
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

    /**
     * 获取消费者记录
     * 
     * @return 消费者记录
     */
    public List<ConsumerRecord<String, byte[]>> getRecords() {
        return records;
    }

    /**
     * 获取主题分区偏移量
     * 
     * @return 主题分区偏移量
     */
    public List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> getKtpOffsets() {
        return ktpOffsets;
    }
}
