package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.AppServer;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.persistece.SourceDataPersistenceServer;
import cn.berry.rapids.data.source.SourceParserGenerator;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

/**
 * Kafka数据源解析器生成器类
 * 
 * 描述: 负责生成Kafka数据源解析器，并将其与事件总线关联。
 * 此类实现了生命周期接口，管理生成器的启动和停止。
 * 
 * 特性:
 * 1. 生成Kafka数据源解析器
 * 2. 将解析器与事件总线关联
 * 
 * @author Berry
 * @version 1.0.0
 */
public class KafkaSourceParserGenerator extends SourceParserGenerator {

    private Thread kafkaSourceThread;

    private KafkaSource kafkaSource;

    private final AppServer appServer;

    private final SourceDataPersistenceServer sourceDataPersistenceServer;

    private final ClickHouseClient clickHouseClient;

    /**
     * 构造Kafka数据源解析器生成器
     * 
     * @param appServer 应用服务器
     * @param configuration 应用配置对象
     * @param sourceDataPersistenceServer 基础数据持久化服务
     * @param clickHouseClient ClickHouse客户端
     */
    public KafkaSourceParserGenerator(AppServer appServer, Configuration configuration, SourceDataPersistenceServer sourceDataPersistenceServer,
                                      ClickHouseClient clickHouseClient) {
        super("parser-kafka", configuration);
        this.appServer = appServer;
        this.clickHouseClient = clickHouseClient;
        this.sourceDataPersistenceServer = sourceDataPersistenceServer;
    }

    /**
     * 启动Kafka数据源解析器生成器
     * 
     * @throws Exception 启动过程中可能发生的任何异常
     */
    @Override
    public void start() throws Exception {

        int coreCnt = configuration.getSystemConfig().getData().getParseThreadSize();
        if (coreCnt <= 0)
            coreCnt = appServer.getCoreCnt();
        this.kafkaSource = new KafkaSource(configuration);
        for (int i = 0; i < coreCnt; i++) {
            KafkaSourceParser kafkaSourceParser = new KafkaSourceParser(configuration,
                    new KafkaSourceProcessor(configuration, sourceDataPersistenceServer, clickHouseClient), kafkaSource);
            addExecutable(kafkaSourceParser);
        }
        this.kafkaSourceThread = new Thread(this.kafkaSource);
        this.kafkaSourceThread.start();
        super.start();
    }

    /**
     * 停止Kafka数据源解析器生成器
     * 
     * @throws Exception 停止过程中可能发生的任何异常
     */
    @Override
    public void stop() throws Exception {
        if (null != this.kafkaSource)
            this.kafkaSource.stop();
        if (null != this.kafkaSourceThread && this.kafkaSourceThread.isAlive())
            try {
                this.kafkaSourceThread.join();
            } catch (Throwable e) {
                //ignore
            }
        super.stop();
    }
}
