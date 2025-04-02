package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kafka主题分区偏移量管理器类
 * 
 * 描述: 负责管理Kafka主题分区的偏移量信息。
 * 
 * 特性:
 * 1. 存储主题分区偏移量
 * 2. 提供偏移量的更新和查询方法
 * 
 * @author Berry
 * @version 1.0.0
 */
public class KafkaTopicPartitionOffsetManager {

    private volatile ConcurrentHashMap<Integer, KafkaTopicPartitionOffsetFastHolder> topicPartitionCache;

    private volatile KafkaTopicPartitionOffsetFastHolder[] holders;

    private final Object lock = new Object();

    /**
     * 获取下一个偏移量
     * 
     * @return 偏移量
     * @throws InterruptedException 中断异常
     */
    public Map<TopicPartition, OffsetAndMetadata> getNextOffsetToCommit() throws InterruptedException {
        return getNextOffsetToCommit(getHolders());
    }

    /**
     * 获取下一个偏移量
     * 
     * @param holders 偏移量持有者数组
     * @return 偏移量
     */
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

    /**
     * 提交偏移量
     * 
     * @param ktp 主题分区
     * @param offsets 偏移量列表
     * @throws InterruptedException 中断异常
     */
    public void commitOffset(KafkaTopicPartition ktp, List<KafkaTopicPartitionOffset> offsets) throws InterruptedException {

        for (KafkaTopicPartitionOffset offset : offsets) {
            offset.complete();
        }
        ConcurrentHashMap<Integer, KafkaTopicPartitionOffsetFastHolder> cache = getCache();
        KafkaTopicPartitionOffsetFastHolder holder = cache.get(ktp.hashCode());
        if (null != holder)
            holder.refreshOffset();
    }

    /**
     * 添加持有者
     * 
     * @param tps 主题分区集合
     */
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

    /**
     * 获取缓存
     * 
     * @return 缓存
     * @throws InterruptedException 中断异常
     */
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

    /**
     * 获取持有者
     * 
     * @return 持有者
     * @throws InterruptedException 中断异常
     */
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

    /**
     * 记录偏移量
     * 
     * @param records 消费者记录
     * @return 偏移量元数据
     * @throws InterruptedException 中断异常
     */
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

    /**
     * 关闭
     * 
     * @return 持有者数组
     */
    public KafkaTopicPartitionOffsetFastHolder[] close() {
        for (KafkaTopicPartitionOffsetFastHolder holder : this.holders) {
            holder.close();
        }
        return holders;
    }
}
