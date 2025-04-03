package cn.berry.rapids.data.source.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaTopicPartitionRecord {

    private final KafkaTopicPartitionOffset ktpOffset;

    private final ConsumerRecord<String, byte[]> record;

    public KafkaTopicPartitionRecord(KafkaTopicPartitionOffset ktpOffset, ConsumerRecord<String, byte[]> record) {
        this.ktpOffset = ktpOffset;
        this.record = record;
    }

    public KafkaTopicPartitionOffset getKtpOffset() {
        return ktpOffset;
    }

    public ConsumerRecord<String, byte[]> getRecord() {
        return record;
    }
}
