package cn.berry.rapids.data.source.kafka;

import java.util.List;

public class KafkaTopicPartitionRecords {

    private final KafkaTopicPartition ktp;

    private final List<KafkaTopicPartitionRecord> records;

    public KafkaTopicPartitionRecords(KafkaTopicPartition ktp, List<KafkaTopicPartitionRecord> records) {
        this.ktp = ktp;
        this.records = records;
    }

    public void addRecords(List<KafkaTopicPartitionRecord> records) {
        this.records.addAll(records);
    }

    public KafkaTopicPartition getKtp() {
        return ktp;
    }

    public List<KafkaTopicPartitionRecord> getRecords() {
        return records;
    }
}
