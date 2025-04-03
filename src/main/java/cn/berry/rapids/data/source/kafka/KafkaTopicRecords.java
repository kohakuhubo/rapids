package cn.berry.rapids.data.source.kafka;

import java.util.ArrayList;
import java.util.List;

public class KafkaTopicRecords {

    private final String topic;

    private final List<KafkaTopicPartitionRecords> partitionRecords;

    public KafkaTopicRecords(String topic) {
        this.topic = topic;
        this.partitionRecords = new ArrayList<>();
    }

    public void addPartitionRecords(KafkaTopicPartitionRecords partitionRecords) {
        this.partitionRecords.add(partitionRecords);
    }

    public String getTopic() {
        return topic;
    }

    public List<KafkaTopicPartitionRecords> getPartitionRecords() {
        return partitionRecords;
    }
}
