package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.AppServer;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.persistece.BaseDataPersistenceServer;
import cn.berry.rapids.data.source.SourceParserGenerator;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

public class KafkaSourceParserGenerator extends SourceParserGenerator {

    private Thread kafkaSourceThread;

    private KafkaSource kafkaSource;

    private final AppServer appServer;

    private final BaseDataPersistenceServer baseDataPersistenceServer;

    private final ClickHouseClient clickHouseClient;

    public KafkaSourceParserGenerator(AppServer appServer, Configuration configuration, BaseDataPersistenceServer baseDataPersistenceServer,
                                      ClickHouseClient clickHouseClient) {
        super("parser-kafka", configuration);
        this.appServer = appServer;
        this.clickHouseClient = clickHouseClient;
        this.baseDataPersistenceServer = baseDataPersistenceServer;
    }

    @Override
    public void start() throws Exception {

        int coreCnt = configuration.getSystemConfig().getData().getParseThreadSize();
        if (coreCnt <= 0)
            coreCnt = appServer.getCoreCnt();
        this.kafkaSource = new KafkaSource(configuration);
        for (int i = 0; i < coreCnt; i++) {
            KafkaSourceParser kafkaSourceParser = new KafkaSourceParser(configuration,
                    new KafkaSourceProcessor(configuration, baseDataPersistenceServer, clickHouseClient), kafkaSource);
            addExecutable(kafkaSourceParser);
        }
        this.kafkaSourceThread = new Thread(this.kafkaSource);
        this.kafkaSourceThread.start();
        super.start();
    }

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
