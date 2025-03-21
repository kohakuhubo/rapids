package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.CycleLife;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class KafkaFetcher implements CycleLife {

    private final KafkaConsumer<String, byte[]> consumer;

    private final Duration timeout;

    private final List<String> subTopics;

    private OffsetCommitCallback commitCallback;

    private volatile Set<TopicPartition> topicPartitions;

    public KafkaFetcher(Properties properties, Duration timeout, List<String> subTopics, OffsetCommitCallback commitCallback) {
        this.consumer = new KafkaConsumer<>(properties);
        this.timeout = timeout;
        this.subTopics = subTopics;
        this.commitCallback = commitCallback;
    }

    public ConsumerRecords<String, byte[]> poll(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (null == offsets || offsets.isEmpty()) {
            this.consumer.commitAsync(offsets, commitCallback);
        }
        return this.consumer.poll(this.timeout);
    }

    public ConsumerRecords<String, byte[]> poll() {
        return this.consumer.poll(this.timeout);
    }

    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.consumer.commitAsync(offsets, commitCallback);
    }

    public void commit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.consumer.commitSync(offsets);
    }

    private void refreshTopicPartitions() {
        Set<TopicPartition> tmp = new HashSet<>();
        for (int i = 0; i < this.subTopics.size(); i++) {
            List<PartitionInfo> partitionInfos = this.consumer.partitionsFor(this.subTopics.get(i));
            if (null != partitionInfos && !partitionInfos.isEmpty()) {
                for (PartitionInfo partitionInfo : partitionInfos) {
                    tmp.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
                }
            }
        }
        this.consumer.assign(tmp);
        this.topicPartitions = tmp;
    }

    @Override
    public void start() throws Exception {
        refreshTopicPartitions();
        for (TopicPartition topicPartition : this.topicPartitions) {
            this.consumer.position(topicPartition);
        }
    }

    public Set<TopicPartition> getTopicPartitions() {
        return this.topicPartitions;
    }

    @Override
    public void stop() throws Exception {
        this.consumer.close();
    }
}
