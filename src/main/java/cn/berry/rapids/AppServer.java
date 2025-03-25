package cn.berry.rapids;

import cn.berry.rapids.aggregate.AggregateServer;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.SourceParserServer;
import cn.berry.rapids.data.persistece.BaseDataPersistenceServer;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

public class AppServer implements CycleLife {

    private final Configuration configuration;

    private final AggregateServer aggregateServiceHandler;

    private final BaseDataPersistenceServer baseDataPersistenceServer;

    private final SourceParserServer sourceParserServer;

    private final int coreCnt;

    private final long startTimestamp;

    private final ClickHouseClient clickHouseClient;

    public AppServer(Configuration configuration) throws Exception {
        this.clickHouseClient = new ClickHouseClient.Builder()
                .config(configuration.getClickHouseClientConfig()).build();
        this.startTimestamp = System.currentTimeMillis();
        this.coreCnt = Runtime.getRuntime().availableProcessors();
        this.configuration = configuration;
        this.aggregateServiceHandler = new AggregateServer(this, configuration, this.clickHouseClient);
        this.baseDataPersistenceServer = new BaseDataPersistenceServer(configuration, this.aggregateServiceHandler, this.clickHouseClient);
        this.sourceParserServer = new SourceParserServer(this, configuration, this.baseDataPersistenceServer);
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public int getCoreCnt() {
        return coreCnt;
    }

    @Override
    public void start() throws Exception {
        this.aggregateServiceHandler.start();
        this.baseDataPersistenceServer.start();
        this.sourceParserServer.start();
    }

    @Override
    public void stop() throws Exception {
        this.sourceParserServer.stop();
        this.baseDataPersistenceServer.stop();
        this.clickHouseClient.close();
        this.aggregateServiceHandler.stop();
    }
}
