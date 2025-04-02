package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.Pair;
import cn.berry.rapids.configuration.BlockConfig;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.persistece.BaseDataPersistenceServer;
import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.data.source.SourceProcessor;
import cn.berry.rapids.definition.BaseDataDefinition;
import cn.berry.rapids.definition.ColumnDataDefinition;
import cn.berry.rapids.enums.SourceTypeEnum;
import cn.berry.rapids.model.BaseData;
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

    private final BaseDataPersistenceServer baseDataPersistenceServer;

    private final ClickHouseClient client;

    private static final byte[] DEFAULT_STRING = "".getBytes(StandardCharsets.UTF_8);

    /**
     * 构造Kafka数据源处理器
     * 
     * @param configuration 应用配置对象
     * @param baseDataPersistenceServer 基础数据持久化服务
     * @param client ClickHouse客户端
     */
    public KafkaSourceProcessor(Configuration configuration, BaseDataPersistenceServer baseDataPersistenceServer,
                                ClickHouseClient client) {
        this.configuration = configuration;
        this.baseDataPersistenceServer = baseDataPersistenceServer;
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
        List<ConsumerRecord<String, byte[]>> records = sourceEntry.getRecords();
        List<Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>>> ktpOffsets = sourceEntry.getKtpOffsets();
        BlockConfig blockConfig = configuration.getSystemConfig().getBlock();

        Map<String, List<ConsumerRecord<String, byte[]>>> recordsMap = records.stream().collect(Collectors.groupingBy(ConsumerRecord::topic));

        for (Pair<KafkaTopicPartition, List<KafkaTopicPartitionOffset>> ktpOffset : ktpOffsets) {
            KafkaTopicPartition ktp = ktpOffset.getKey();
            String topic = ktp.getTopic();
            List<KafkaTopicPartitionOffset> ktpOffsetList = ktpOffset.getValue();
            List<ConsumerRecord<String, byte[]>> topicRecord = recordsMap.get(topic);
            BaseDataDefinition baseDataDefinition = configuration.getBaseDataDefinition(SourceTypeEnum.KAFKA, topic);

            Block block;
            try {
                block = client.createBlock(baseDataDefinition.getTableName());
            } catch (Exception e) {
                continue;
            }
            BaseData baseData = new BaseData(blockConfig.getBatchDataMaxRowCnt(), blockConfig.getBatchDataMaxByteSize(), block);

            ColumnDataDefinition[] columnDataDefinitions = baseDataDefinition.getColumnDataDefinitions();

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
                byte[] bytes = record.value();
                int offset = 0;
                for (ColumnDataDefinition columnDataDefinition : columnDataDefinitions) {
                    String columnName = columnDataDefinition.getName();
                    if (null == columnName || columnName.trim().isEmpty()) {
                        //没有列明，不解析，直接跳过
                        offset = skip(columnDataDefinition.getClazz(), bytes, offset);
                    } else {
                        try {
                            //写入block
                            offset = write(columnDataDefinition.getClazz(), bytes, offset, block, columnName);
                        } catch (SQLException | IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                //判断block是否已经写满
                if (baseData.isFull()) {
                    //设置数据的kafka位移信息
                    baseData.setSourceEntry(new KafkaSourceEntry(null, sourceEntry.getKafkaSource(),
                            Collections.singletonList(new Pair<>(ktp, commitPartitionOffsetList))));
                    //提交数据
                    baseDataPersistenceServer.handle(baseData);
                    //重新初始化block
                    try {
                        block = client.createBlock(baseDataDefinition.getTableName());
                    } catch (Exception e) {
                        continue;
                    }
                    //重新初始化baseData
                    baseData = new BaseData(blockConfig.getBatchDataMaxRowCnt(), blockConfig.getBatchDataMaxByteSize(), block);
                }
            }
            //检查是否还有未提交数据
            if (!baseData.isFull()) {
                baseDataPersistenceServer.handle(baseData);
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
