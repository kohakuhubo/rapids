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

public class KafkaSource extends Stoppable implements Runnable, Source<KafkaSourceEntry> {

    private KafkaFetcher kafkaFetcher;

    private final Properties properties;

    private final KafkaSourceHandover sourceHandover;

    private final KafkaTopicPartitionOffsetManager offsetManager;

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

    private String generateClientId(KafkaConfig kafkaConfig) {
        if (null != kafkaConfig.getClientId() && !"".equals(kafkaConfig.getClientId())) {
            return kafkaConfig.getClientId();
        } else {
            return kafkaConfig.getAppName() + "-" + kafkaConfig.getGroupId() + "-"
                    + getHostAddress() + "-" + System.currentTimeMillis();
        }
    }

    private static String getHostAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Throwable e) {
            return "";
        }
    }

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
                        Pair<List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>>, List<ConsumerRecord<String, byte[]>>> pair = offsetManager.recordOffset(records);
                        if (null != pair.key() && !pair.key().isEmpty()) {
                            sourceEntry = new KafkaSourceEntry(pair.value(), this, pair.key());
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

    @Override
    public boolean continuable() {
        return !isTerminal();
    }

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

    @Override
    public void commit(SourceEntry<KafkaSourceEntry> entry) {
        KafkaSourceEntry kafkaSourceEntry = entry.entry();
        for (Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>> ktpOffsets : kafkaSourceEntry.getKtpOffsets()) {
            try {
                offsetManager.commitOffset(ktpOffsets.key(), ktpOffsets.value());
            } catch (InterruptedException e) {
                //error info
            }
        }
    }
}
