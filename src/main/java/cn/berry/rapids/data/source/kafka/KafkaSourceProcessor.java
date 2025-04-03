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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Kafka数据源处理器类
 * 
 * 描述: 负责处理从Kafka消费者中读取的数据，并将其传递给数据解析服务。
 * 此类实现了生命周期接口，管理处理器的启动和停止。
 * 
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

    /**
     * 构造Kafka数据源处理器
     * 
     * @param configuration 应用配置对象
     * @param sourceDataPersistenceServer 基础数据持久化服务
     * @param client ClickHouse客户端
     */
    public KafkaSourceProcessor(Configuration configuration, SourceDataPersistenceServer sourceDataPersistenceServer,
                                ClickHouseClient client) {
        this.configuration = configuration;
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

        List<KafkaTopicRecords> kafkaTopicRecordsList = sourceEntry.getKafkaTopicRecordsList();

        BlockConfig blockConfig = configuration.getSystemConfig().getBlock();

        Map<String, List<ConsumerRecord<String, byte[]>>> recordsMap = records.stream().collect(Collectors.groupingBy(ConsumerRecord::topic));

        for (KafkaTopicRecords kafkaTopicRecords : kafkaTopicRecordsList) {
            //主题
            String topic = kafkaTopicRecords.getTopic();
            //获取主题对应的数据定义信息
            SourceDataDefinition sourceDataDefinition = configuration.getSourceDataDefinition(SourceTypeEnum.KAFKA, topic);
            //列数据定义
            ColumnDataDefinition[] columnDataDefinitions = sourceDataDefinition.getColumnDataDefinitions();
            Block block;
            try {
                block = client.createBlock(sourceDataDefinition.getTableName());
            } catch (Exception e) {
                logger.error("create block has error!", e);
                continue;
            }
            //创建源数据事件
            SourceDataEvent sourceDataEvent = new SourceDataEvent(topic, blockConfig.getBatchDataMaxRowCnt(),
                    blockConfig.getBatchDataMaxByteSize(), block);
            //待提交位移
            List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> commitOffset = new ArrayList<>();
            for (KafkaTopicPartitionRecords kafkaTopicPartitionRecords : kafkaTopicRecords.getPartitionRecords()) {
                //分区
                KafkaTopicPartition ktp = kafkaTopicPartitionRecords.getKtp();
                List<KafkaTopicPartitionOffset> ktpOffsetList = new ArrayList<>();
                //消息
                List<KafkaTopicPartitionRecord> records = kafkaTopicPartitionRecords.getRecords();
                for (KafkaTopicPartitionRecord kafkaTopicPartitionRecord : records) {
                    //消息
                    ConsumerRecord<String, byte[]> record = kafkaTopicPartitionRecord.getRecord();
                    //位移
                    KafkaTopicPartitionOffset ktpOffset = kafkaTopicPartitionRecord.getKtpOffset();
                }
            }

            int commitedSize = 0;
            for (int i = 0; i < topicRecord.size(); i++, commitedSize++) {

                List<KafkaTopicPartitionOffset> commitPartitionOffsetList;
                if (commitedSize == 0) {
                    commitPartitionOffsetList = new ArrayList<>(topicRecord.size());
                } else {
                    commitPartitionOffsetList = new ArrayList<>(topicRecord.size() - commitedSize);
                }

                ConsumerRecord<String, byte[]> record = topicRecord.get(i);
                commitPartitionOffsetList.add(ktpOffsetList.get(i));
                try {
                    parseColumns(block, columnDataDefinitions, record);
                } catch (Throwable e) {
                    logger.error("parse columns has error! topic: {} partition: {} offset: {}", record.topic(), record.partition(),
                            record.offset(), e);
                    break;
                }
                //判断block是否已经写满
                if (sourceDataEvent.isFull()) {
                    //设置数据的kafka位移信息
                    sourceDataEvent.setSourceEntry(new KafkaSourceEntry(null, sourceEntry.getKafkaSource(),
                            Collections.singletonList(new Pair<>(ktp, commitPartitionOffsetList))));
                    //提交数据
                    sourceDataPersistenceServer.handle(sourceDataEvent);
                    //重新初始化block
                    try {
                        block = client.createBlock(sourceDataDefinition.getTableName());
                    } catch (Exception e) {
                        logger.error("create block has error!", e);
                        continue;
                    }
                    //重新初始化sourceDataEvent
                    sourceDataEvent = new SourceDataEvent(topic, blockConfig.getBatchDataMaxRowCnt(), blockConfig.getBatchDataMaxByteSize(), block);
                }
            }
            //检查是否还有未提交数据
            if (null != sourceDataEvent && !sourceDataEvent.isFull()) {
                sourceDataPersistenceServer.handle(sourceDataEvent);
            }
        }
    }

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
     * @param type 数据类型
     * @param bytes 字节数组
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
     * @param type 数据类型
     * @param bytes 字节数组
     * @param offset 偏移量
     * @param block 数据块
     * @param columnName 列名
     * @return 新的偏移量
     * @throws SQLException SQL异常
     * @throws IOException IO异常
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
