package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.Pair;
import cn.berry.rapids.data.source.SourceEntry;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public class KafkaSourceEntry implements SourceEntry<KafkaSourceEntry> {

    private final List<ConsumerRecord<String, byte[]>> records;

    private final KafkaSource kafkaSource;

    private final List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> ktpOffsets;

    public KafkaSourceEntry(List<ConsumerRecord<String, byte[]>> records, KafkaSource kafkaSource, List<Pair<KafkaTopicPartition,
            List<KafkaTopicPartitionOffset>>> ktpOffsets) {
        this.records = records;
        this.kafkaSource = kafkaSource;
        this.ktpOffsets = ktpOffsets;
    }

    @Override
    public long id() {
        return 0;
    }

    @Override
    public KafkaSourceEntry entry() {
        return this;
    }

    @Override
    public void success() {
        this.kafkaSource.commit(this);
    }

    @Override
    public void fail() {

    }

    public KafkaSource getKafkaSource() {
        return kafkaSource;
    }

    public List<ConsumerRecord<String, byte[]>> getRecords() {
        return records;
    }

    public List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> getKtpOffsets() {
        return ktpOffsets;
    }
}
