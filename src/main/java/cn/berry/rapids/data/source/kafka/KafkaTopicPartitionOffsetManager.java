package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaTopicPartitionOffsetManager {

    private volatile ConcurrentHashMap<Integer, KafkaTopicPartitionOffsetFastHolder> topicPartitionCache;

    private volatile KafkaTopicPartitionOffsetFastHolder[] holders;

    private final Object lock = new Object();

    public Map<TopicPartition, OffsetAndMetadata> getNextOffsetToCommit() throws InterruptedException {
        return getNextOffsetToCommit(getHolders());
    }

    public Map<TopicPartition, OffsetAndMetadata> getNextOffsetToCommit(KafkaTopicPartitionOffsetFastHolder[] holders) {
        if (null == holders || holders.length == 0) {
            return Collections.emptyMap();
        }
        Map<TopicPartition, OffsetAndMetadata> offsetAndMetadata = null;
        for (KafkaTopicPartitionOffsetFastHolder holder : holders) {
            KafkaTopicPartitionOffset commitOffset = holder.getNewCommitOffset();
            if (null != commitOffset) {
                if (null == offsetAndMetadata) {
                    offsetAndMetadata = new HashMap<>(holders.length);
                }
                offsetAndMetadata.put(holder.getTopicPartition(), new OffsetAndMetadata(commitOffset.getOffset() + 1));
            }
        }
        return null == offsetAndMetadata ? Collections.emptyMap() : offsetAndMetadata;
    }

    public void commitOffset(KafkaTopicPartition ktp, List<KafkaTopicPartitionOffset> offsets) throws InterruptedException {

        for (KafkaTopicPartitionOffset offset : offsets) {
            offset.complete();
        }
        ConcurrentHashMap<Integer, KafkaTopicPartitionOffsetFastHolder> cache = getCache();
        KafkaTopicPartitionOffsetFastHolder holder = cache.get(ktp.hashCode());
        if (null != holder)
            holder.refreshOffset();
    }

    public void addHolder(Collection<TopicPartition> tps) {

        KafkaTopicPartitionOffsetFastHolder[] tmp;
        synchronized (lock) {
            tmp = this.holders;
            this.holders = null;
            this.topicPartitionCache = null;
        }

        KafkaTopicPartitionOffsetFastHolder[] newHolders = new KafkaTopicPartitionOffsetFastHolder[tps.size() + (null == tmp ? 0 : tmp.length)];
        ConcurrentHashMap<Integer, KafkaTopicPartitionOffsetFastHolder> newCache = new ConcurrentHashMap<>(newHolders.length);
        int i = 0;
        for (TopicPartition tp : tps) {
            KafkaTopicPartition ktp = new KafkaTopicPartition(tp.topic(), tp.partition());
            KafkaTopicPartitionOffsetFastHolder holder = new KafkaTopicPartitionOffsetFastHolder(ktp, tp);
            newHolders[i++] = holder;
            newCache.put(ktp.hashCode(), holder);
        }

        if (null != tmp) {
            for (KafkaTopicPartitionOffsetFastHolder holder : tmp) {
                newHolders[i++] = holder;
                newCache.put(holder.hashCode(), holder);
            }
        }

        synchronized (lock) {
            this.topicPartitionCache = newCache;
            this.holders = newHolders;
            this.notifyAll();
        }
    }

    private ConcurrentHashMap<Integer, KafkaTopicPartitionOffsetFastHolder> getCache() throws InterruptedException {
        ConcurrentHashMap<Integer, KafkaTopicPartitionOffsetFastHolder> tmp;
        if ((tmp = this.topicPartitionCache) == null) {
            synchronized (lock) {
                while ((tmp = this.topicPartitionCache) == null) {
                    this.wait();
                }
            }
        }
        return tmp;
    }

    private KafkaTopicPartitionOffsetFastHolder[] getHolders() throws InterruptedException {
        KafkaTopicPartitionOffsetFastHolder[] tmp;
        if ((tmp = this.holders) == null) {
            synchronized (lock) {
                while ((tmp = this.holders) == null) {
                    this.wait();
                }
            }
        }
        return tmp;
    }

    public Pair<List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>>, List<ConsumerRecord<String, byte[]>>> recordOffset(ConsumerRecords<String, byte[]> records) throws InterruptedException {
        List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> offsetMetadata = new ArrayList<>();
        List<ConsumerRecord<String, byte[]>> finalRecords = new ArrayList<>(records.count());
        KafkaTopicPartitionOffsetFastHolder[] holders = getHolders();
        for (KafkaTopicPartitionOffsetFastHolder holder : holders) {
            List<ConsumerRecord<String, byte[]>> tpRecords = records.records(holder.getTopicPartition());
            List<KafkaTopicPartitionOffset> offsets = holder.addOffset(tpRecords);
            if (null != offsets && !offsets.isEmpty()) {
                offsetMetadata.add(new Pair<>(holder.getKTopicPartition(), offsets));
                finalRecords.addAll(tpRecords);
            }
        }
        return new Pair<>(offsetMetadata, finalRecords);
    }

    public KafkaTopicPartitionOffsetFastHolder[] close() {
        for (KafkaTopicPartitionOffsetFastHolder holder : this.holders) {
            holder.close();
        }
        return holders;
    }

}
