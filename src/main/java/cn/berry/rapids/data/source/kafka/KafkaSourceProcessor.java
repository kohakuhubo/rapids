package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.Pair;
import cn.berry.rapids.configuration.BlockConfig;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.persistece.SourceDataPersistenceServer;
import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.data.source.SourceProcessor;
import cn.berry.rapids.definition.SourceDataDefinition;
import cn.berry.rapids.definition.ColumnDataDefinition;
import cn.berry.rapids.enums.SourceTypeEnum;
import cn.berry.rapids.model.SourceDataEvent;
import cn.berry.rapids.util.ByteUtil;
import com.berry.clickhouse.tcp.client.ClickHouseClient;
import com.berry.clickhouse.tcp.client.data.Block;
import com.berry.clickhouse.tcp.client.data.IColumn;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;

/**
 * Kafka数据源处理器类
 * <p>
 * 描述: 负责处理从Kafka消费者中读取的数据，并将其传递给数据解析服务。
 * 此类实现了生命周期接口，管理处理器的启动和停止。
 * <p>
 * 特性:
 * 1. 处理Kafka消费者读取的数据
 * 2. 将数据传递给数据解析服务
 *
 * @author Berry
 * @version 1.0.0
 */
public class KafkaSourceProcessor implements SourceProcessor<KafkaSourceEntry> {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceProcessor.class);

    private final Configuration configuration;

    private final SourceDataPersistenceServer sourceDataPersistenceServer;

    private final ClickHouseClient client;

    private static final byte[] DEFAULT_STRING = "".getBytes(StandardCharsets.UTF_8);

    private final BlockConfig blockConfig;

    /**
     * 构造Kafka数据源处理器
     *
     * @param configuration               应用配置对象
     * @param sourceDataPersistenceServer 基础数据持久化服务
     * @param client                      ClickHouse客户端
     */
    public KafkaSourceProcessor(Configuration configuration, SourceDataPersistenceServer sourceDataPersistenceServer,
                                ClickHouseClient client) {
        this.configuration = configuration;
        this.blockConfig = configuration.getSystemConfig().getBlock();
        this.sourceDataPersistenceServer = sourceDataPersistenceServer;
        this.client = client;
    }

    /**
     * 处理数据源条目
     *
     * @param entry 数据源条目
     */
    @Override
    public void process(SourceEntry<KafkaSourceEntry> entry) {
        KafkaSourceEntry sourceEntry = entry.entry();
        //遍历主题消息
        for (KafkaTopicRecords kafkaTopicRecords : sourceEntry.getKafkaTopicRecordsList()) {
            try {
                //解析主题消息
                parseTopicRecord(sourceEntry.getKafkaSource(), kafkaTopicRecords);
            } catch (Throwable e) {
                logger.error("process error", e);
            }
        }
    }

    /**
     * 解析主题消息
     *
     * @param kafkaSource       kafka源
     * @param kafkaTopicRecords kafka主题消息
     */
    private void parseTopicRecord(KafkaSource kafkaSource, KafkaTopicRecords kafkaTopicRecords) throws Throwable {
        //主题
        String topic = kafkaTopicRecords.getTopic();
        //获取主题对应的数据定义信息
        SourceDataDefinition sourceDataDefinition = configuration.getSourceDataDefinition(SourceTypeEnum.KAFKA, topic);
        //列数据定义
        ColumnDataDefinition[] columnDataDefinitions = sourceDataDefinition.getColumnDataDefinitions();
        //数据块
        Block block = null;
        //分区消息
        List<KafkaTopicPartitionRecords> kafkaTopicPartitionRecordsList = kafkaTopicRecords.getPartitionRecords();
        //待提交位移
        Map<KafkaTopicPartition, List<KafkaTopicPartitionOffset>> commitOffsetMap = new HashMap<>(kafkaTopicPartitionRecordsList.size());
        //遍历分区消息
        for (KafkaTopicPartitionRecords kafkaTopicPartitionRecords : kafkaTopicPartitionRecordsList) {
            //分区
            KafkaTopicPartition ktp = kafkaTopicPartitionRecords.getKtp();
            //消息
            List<KafkaTopicPartitionRecord> records = kafkaTopicPartitionRecords.getRecords();
            //获取待提交位移集合
            List<KafkaTopicPartitionOffset> offsets = commitOffsetMap.computeIfAbsent(ktp, ktp1 -> new ArrayList<>(records.size()));
            for (KafkaTopicPartitionRecord kafkaTopicPartitionRecord : records) {
                //消息
                ConsumerRecord<String, byte[]> record = kafkaTopicPartitionRecord.getRecord();
                //初始化数据块
                if (null == block) {
                    //重新初始化block
                    try {
                        block = client.createBlock(sourceDataDefinition.getTableName());
                    } catch (Exception e) {
                        throw new RuntimeException(String.format("create block[%s] error!", sourceDataDefinition.getTableName()), e);
                    }
                }
                try {
                    //解析列数据
                    parseColumns(block, columnDataDefinitions, kafkaTopicPartitionRecord.getRecord());
                    //记录位移
                    offsets.add(kafkaTopicPartitionRecord.getKtpOffset());
                } catch (Throwable e) {
                    throw new RuntimeException(String.format("parse columns has error! topic: %s partition: %s offset: %s", record.topic(), record.partition(),
                            record.offset()), e);
                }
                //判断block是否已经写满
                if (isFull(block)) {
                    //提交数据
                    commit(topic, block, kafkaSource, commitOffsetMap);
                    block = null;
                }
            }
        }
        //判断block是否为空
        if (isNotEmpty(block)) {
            //提交数据
            commit(topic, block, kafkaSource, commitOffsetMap);
        }
    }

    /**
     * 提交数据
     *
     * @param topic           主题
     * @param block           数据块
     * @param kafkaSource     kafka数据源
     * @param commitOffsetMap 提交位移集合
     */
    private void commit(String topic, Block block, KafkaSource kafkaSource, Map<KafkaTopicPartition, List<KafkaTopicPartitionOffset>> commitOffsetMap) {
        //设置数据的kafka位移信息
        KafkaSourceEntry commitKafkaSourceEntry = new KafkaSourceEntry(kafkaSource);
        List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> commitOffsets = commitOffsetMap.entrySet().stream()
                .map(e -> new Pair<>(e.getKey(), e.getValue())).toList();
        //清除原有数据
        commitOffsetMap.clear();
        commitKafkaSourceEntry.setCommitOffset(commitOffsets);
        //提交数据
        sourceDataPersistenceServer.handle(new SourceDataEvent(topic, block, commitKafkaSourceEntry));
    }

    private boolean isNotEmpty(Block block) {
        return null != block && block.rowCnt() > 0;
    }

    /**
     * 判断数据块是否已经写满
     *
     * @param block 数据块
     */
    private boolean isFull(Block block) {
        return block.rowCnt() >= blockConfig.getBatchDataMaxRowCnt()
                || block.readBytes() >= blockConfig.getBatchDataMaxByteSize();
    }

    /**
     * 解析列数据
     *
     * @param block                 数据块
     * @param columnDataDefinitions 列数据定义
     * @param record                消息
     * @throws Throwable 解析异常
     */
    private void parseColumns(Block block, ColumnDataDefinition[] columnDataDefinitions,
                              ConsumerRecord<String, byte[]> record) throws Throwable {

        byte[] bytes = record.value();
        int offset = 0;
        while (offset < bytes.length) {
            for (ColumnDataDefinition columnDataDefinition : columnDataDefinitions) {
                String columnName = columnDataDefinition.getName();
                if (null == columnName || columnName.trim().isEmpty()) {
                    //没有列明，不解析，直接跳过
                    offset = skip(columnDataDefinition.getClazz(), bytes, offset);
                } else {
                    //写入block
                    offset = write(columnDataDefinition.getClazz(), bytes, offset, block, columnName);
                }
            }
        }
    }

    /**
     * 跳过数据类型
     *
     * @param type   数据类型
     * @param bytes  字节数组
     * @param offset 偏移量
     * @return 新的偏移量
     */
    private static int skip(Class<?> type, byte[] bytes, int offset) {
        if (type == byte.class || type == Byte.class) return offset + Byte.BYTES;
        if (type == short.class || type == Short.class) return offset + Short.BYTES;
        if (type == int.class || type == Integer.class) return offset + Integer.BYTES;
        if (type == long.class || type == Long.class) return offset + Long.BYTES;
        if (type == float.class || type == Float.class) return offset + Float.BYTES;
        if (type == double.class || type == Double.class) return offset + Double.BYTES;
        if (type == String.class) return offset + ByteUtil.readInt32be(bytes, offset) + Integer.BYTES;
        throw new IllegalArgumentException("不支持的数据类型: " + type.getName());
    }

    /**
     * 写入数据类型
     *
     * @param type       数据类型
     * @param bytes      字节数组
     * @param offset     偏移量
     * @param block      数据块
     * @param columnName 列名
     * @return 新的偏移量
     * @throws SQLException SQL异常
     * @throws IOException  IO异常
     */
    private static int write(Class<?> type, byte[] bytes, int offset, Block block, String columnName) throws SQLException, IOException {
        IColumn column = block.getColumn(columnName);
        if (type == byte.class || type == Byte.class) {
            column.write(bytes, offset, Byte.BYTES);
            return offset + Byte.BYTES;
        } else if (type == short.class || type == Short.class) {
            column.write(bytes, offset, Short.BYTES);
            return offset + Short.BYTES;
        } else if (type == int.class || type == Integer.class) {
            column.write(bytes, offset, Integer.BYTES);
            return offset + Integer.BYTES;
        } else if (type == long.class || type == Long.class) {
            column.write(bytes, offset, Long.BYTES);
            return offset + Long.BYTES;
        } else if (type == float.class || type == Float.class) {
            column.write(bytes, offset, Float.BYTES);
            return offset + Float.BYTES;
        } else if (type == double.class || type == Double.class) {
            column.write(bytes, offset, Double.BYTES);
            return offset + Double.BYTES;
        } else if (type == String.class) {
            int len = ByteUtil.readInt32be(bytes, offset);
            if (len == 0) {
                column.write(DEFAULT_STRING, offset, DEFAULT_STRING.length);
            } else {
                column.write(bytes, offset + 4, len);
            }
            return offset + (len + 4);
        }
        throw new IllegalArgumentException("不支持的数据类型: " + type.getName());
    }
}
