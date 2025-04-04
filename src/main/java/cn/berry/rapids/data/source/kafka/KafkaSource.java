package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.Pair;
import cn.berry.rapids.Stoppable;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.configuration.KafkaConfig;
import cn.berry.rapids.data.source.Source;
import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.exception.ClosedException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Kafka数据源类
 * 
 * 描述: 负责从Kafka主题中读取数据，并将其传递给数据解析服务。
 * 此类实现了生命周期接口，管理Kafka消费者的启动和停止。
 * 
 * 特性:
 * 1. 支持从Kafka主题中读取数据
 * 2. 将数据传递给数据解析服务
 * 
 * @author Berry
 * @version 1.0.0
 */
public class KafkaSource extends Stoppable implements Runnable, Source<KafkaSourceEntry> {

    private KafkaFetcher kafkaFetcher;

    private final Properties properties;

    private final KafkaSourceHandover sourceHandover;

    private final KafkaTopicPartitionOffsetManager offsetManager;

    /**
     * 构造Kafka数据源
     * 
     * @param configuration 应用配置对象
     * @throws Exception 初始化过程中可能发生的任何异常
     */
    public KafkaSource(Configuration configuration) throws Exception {
        KafkaConfig kafkaConfig = configuration.getSystemConfig().getSource().getKafka();
        this.sourceHandover = new KafkaSourceHandover();
        this.offsetManager = new KafkaTopicPartitionOffsetManager();

        this.properties = new Properties();
        this.properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.getBootstrapServers());
        this.properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.getBootstrapServers());
        this.properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaConfig.getBootstrapServers());
        this.properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, kafkaConfig.getBootstrapServers());
        this.properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        this.properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, kafkaConfig.getSessionTimeout());
        this.properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, kafkaConfig.getMaxPollIntervalMs());
        this.properties.put(ConsumerConfig.CLIENT_ID_CONFIG, generateClientId(kafkaConfig));
        this.kafkaFetcher = new KafkaFetcher(properties, Duration.ofMillis(kafkaConfig.getPollTimeout()), kafkaConfig.getSubTopics(),
                (offset, exception) -> {
                    if (null != exception) {
                        //error info
                    } else {
                        //success info
                    }
                });
        this.kafkaFetcher.start();
        Set<TopicPartition> topicPartitions = this.kafkaFetcher.getTopicPartitions();
        if (null != topicPartitions && !topicPartitions.isEmpty()) {
            offsetManager.addHolder(topicPartitions);
        }
    }

    /**
     * 生成客户端ID
     * 
     * @param kafkaConfig Kafka配置对象
     * @return 客户端ID
     */
    private String generateClientId(KafkaConfig kafkaConfig) {
        if (null != kafkaConfig.getClientId() && !"".equals(kafkaConfig.getClientId())) {
            return kafkaConfig.getClientId();
        } else {
            return kafkaConfig.getAppName() + "-" + kafkaConfig.getGroupId() + "-"
                    + getHostAddress() + "-" + System.currentTimeMillis();
        }
    }

    /**
     * 获取主机地址
     * 
     * @return 主机地址
     */
    private static String getHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Throwable e) {
            return "";
        }
    }

    /**
     * 运行Kafka数据源
     */
    @Override
    public void run() {
        if (isTerminal()) {
            return;
        }

        try {
            KafkaSourceEntry sourceEntry = null;
            while (!isTerminal() && !Thread.currentThread().isInterrupted()) {
                Map<TopicPartition, OffsetAndMetadata> nextOffsetToCommit = offsetManager.getNextOffsetToCommit();
                if (null != nextOffsetToCommit && !nextOffsetToCommit.isEmpty()) {
                    this.kafkaFetcher.commitAsync(nextOffsetToCommit);
                }

                if (null == sourceEntry) {
                    ConsumerRecords<String, byte[]> records = kafkaFetcher.poll();
                    if (null != records && !records.isEmpty()) {
                        List<KafkaTopicRecords> kafkaTopicRecordsList = offsetManager.recordOffset(records);
                        if (!kafkaTopicRecordsList.isEmpty()) {
                            sourceEntry = new KafkaSourceEntry(this, kafkaTopicRecordsList);
                        }
                    }
                }

                if (null != sourceEntry) {
                    try {
                        sourceHandover.produce(sourceEntry);
                        sourceEntry = null;
                    } catch (ClosedException e) {
                        //ignore
                    }
                }
            }
        } catch (ClosedException e) {
            //ignore
        } catch (Throwable e) {
            //ignore
            sourceHandover.reportError(e);
        } finally {
            sourceHandover.stop();
            KafkaTopicPartitionOffsetFastHolder[] holders = this.offsetManager.close();
            if (null != holders && holders.length > 0) {
                doFinalOffsetCommit(holders);
            }
            try {
                this.kafkaFetcher.stop();
            } catch (Exception e) {
                //ignore
            }
        }
    }

    /**
     * 执行最终的偏移量提交
     * 
     * @param holders 偏移量持有者数组
     */
    private void doFinalOffsetCommit(KafkaTopicPartitionOffsetFastHolder[] holders) {
        for (KafkaTopicPartitionOffsetFastHolder holder : holders) {
            while (!holder.isFinish()) {
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }

        Map<TopicPartition, OffsetAndMetadata> nextOffsetToCommit = offsetManager.getNextOffsetToCommit(holders);
        if (null != nextOffsetToCommit && !nextOffsetToCommit.isEmpty()) {
            kafkaFetcher.commit(nextOffsetToCommit);
        }
    }

    /**
     * 检查是否可继续
     * 
     * @return 是否可继续
     */
    @Override
    public boolean continuable() {
        return !isTerminal();
    }

    /**
     * 获取下一个数据源条目
     * 
     * @return 数据源条目
     */
    @Override
    public SourceEntry<KafkaSourceEntry> next() {
        try {
            return sourceHandover.pollNext();
        } catch (ClosedException e) {
            //error info
        } catch (Throwable e) {
            //error info
            stop();
        }
        return null;
    }

    /**
     * 提交数据源条目
     * 
     * @param entry 数据源条目
     */
    @Override
    public void commit(SourceEntry<KafkaSourceEntry> entry) {
        KafkaSourceEntry kafkaSourceEntry = entry.entry();
        for (Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>> ktpOffsets : kafkaSourceEntry.getCommitOffset()) {
            try {
                offsetManager.commitOffset(ktpOffsets.getKey(), ktpOffsets.getValue());
            } catch (InterruptedException e) {
                //error info
            }
        }
    }
}
