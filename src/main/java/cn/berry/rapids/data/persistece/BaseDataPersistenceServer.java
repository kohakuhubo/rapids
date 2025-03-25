package cn.berry.rapids.data.persistece;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.aggregate.AggregateServiceHandler;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.configuration.DataConfig;
import cn.berry.rapids.eventbus.EventBus;
import cn.berry.rapids.eventbus.EventBusBuilder;
import cn.berry.rapids.eventbus.Subscription;
import cn.berry.rapids.model.BaseData;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

public class BaseDataPersistenceServer implements BaseDataPersistenceHandler<BaseData>, CycleLife {

    private final Configuration configuration;

    private EventBus eventBus;

    private final AggregateServiceHandler aggregateServiceHandler;

    private final ClickHouseClient clickHouseClient;

    public BaseDataPersistenceServer(Configuration configuration, AggregateServiceHandler aggregateServiceHandler, ClickHouseClient clickHouseClient) {
        this.clickHouseClient = clickHouseClient;
        this.configuration = configuration;
        this.aggregateServiceHandler = aggregateServiceHandler;
    }

    @Override
    public void handle(BaseData baseData) {
        eventBus.postAsync(baseData, 500L);
    }

    @Override
    public void start() throws Exception {
        DataConfig dataConfig = configuration.getSystemConfig().getData();
        EventBusBuilder eventBusBuilder = EventBus.newEventBusBuilder().eventType("base")
                .queueSize(dataConfig.getDataInsertQueue())
                .threadName("base-data-persistence-server");
        for (int i = 0; i < dataConfig.getDataInsertThreadSize(); i++) {
            eventBusBuilder.subscription((Subscription) new DefaultBaseDataPersistenceHandler(i, configuration, clickHouseClient, aggregateServiceHandler));
        }
        eventBusBuilder.queueSize(dataConfig.getDataInsertQueueNumber())
                .threadName("base-data-persistence-server-number");
        this.eventBus = eventBusBuilder.build(configuration);
    }

    @Override
    public void stop() throws Exception {
        if (this.eventBus != null)
            this.eventBus.stop();
    }
}
