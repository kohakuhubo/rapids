package cn.berry.rapids.data.source.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class KafkaTopicPartitionOffsetFastHolder {

    private final KafkaTopicPartition kTopicPartition;

    private final TopicPartition topicPartition;

    private final TreeMap<Long, KafkaTopicPartitionOffset> offsets;

    private volatile KafkaTopicPartitionOffset currentOffset;

    private final AtomicReference<KafkaTopicPartitionOffset> commitOffset = new AtomicReference<>();

    private volatile boolean isClose = false;

    public KafkaTopicPartitionOffsetFastHolder(KafkaTopicPartition kTopicPartition, TopicPartition topicPartition) {
        this.kTopicPartition = kTopicPartition;
        this.topicPartition = topicPartition;
        this.offsets = new TreeMap<>((o1, o2) -> (int) (o1 - o2));
    }

    public List<KafkaTopicPartitionOffset> addOffset(List<ConsumerRecord<String, byte[]>> tpRecords) {
        List<KafkaTopicPartitionOffset> offsets;
        synchronized (this) {
            if (isClose) {
                return Collections.emptyList();
            } else {
                offsets = new ArrayList<>(tpRecords.size());
                for (ConsumerRecord<String, byte[]> tpRecord : tpRecords) {
                    KafkaTopicPartitionOffset offset = new KafkaTopicPartitionOffset(tpRecord.offset());
                    this.offsets.putIfAbsent(tpRecord.offset(), offset);
                    offsets.add(offset);
                }
            }
            return offsets;
        }
    }

    public void refreshOffset() {
        synchronized (this) {
            KafkaTopicPartitionOffset newOffset = null;
            boolean hasNext = true;
            do {
                Map.Entry<Long, KafkaTopicPartitionOffset> entry = offsets.firstEntry();
                if (null != entry && entry.getValue().isComplete()) {
                    newOffset = entry.getValue();
                    offsets.remove(entry.getKey());
                } else {
                    hasNext = false;
                }
            } while (hasNext);

            if (null != newOffset) {
                this.currentOffset = newOffset;
                if (this.commitOffset.getAndSet(this.currentOffset) != null) {
                    // warn info
                }
            }
        }
    }

    public void close() {
        synchronized (this) {
            this.isClose = true;
        }
    }

    public KafkaTopicPartition getKTopicPartition() {
        return kTopicPartition;
    }

    public TopicPartition getTopicPartition() {
        return topicPartition;
    }

    public KafkaTopicPartitionOffset getNewCommitOffset() {
        return commitOffset.getAndSet(null);
    }

    public boolean isClose() {
        return isClose;
    }

    public boolean isFinish() {
        return noMoreCommitOffset() && isClose;
    }

    private boolean noMoreCommitOffset() {
        Map.Entry<Long, KafkaTopicPartitionOffset> entry = null;
        synchronized (this) {
            entry = this.offsets.lastEntry();
        }
        return (null == entry || entry.getValue().isComplete());
    }
}
