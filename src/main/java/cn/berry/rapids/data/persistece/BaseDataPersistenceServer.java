package cn.berry.rapids.data.persistece;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.aggregate.AggregateServiceHandler;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.configuration.DataConfig;
import cn.berry.rapids.definition.BaseDataDefinition;
import cn.berry.rapids.eventbus.Event;
import cn.berry.rapids.eventbus.EventBus;
import cn.berry.rapids.eventbus.EventBusBuilder;
import cn.berry.rapids.eventbus.Subscription;
import cn.berry.rapids.model.BaseData;
import com.berry.clickhouse.tcp.client.ClickHouseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BaseDataPersistenceServer implements CycleLife {

    /**
     * 日志记录器
     */
    private static final Logger logger = LoggerFactory.getLogger(BaseDataPersistenceServer.class);

    private final Configuration configuration;

    private EventBus eventBus;

    private final AggregateServiceHandler aggregateServiceHandler;

    private final ClickHouseClient clickHouseClient;

    public BaseDataPersistenceServer(Configuration configuration, AggregateServiceHandler aggregateServiceHandler, ClickHouseClient clickHouseClient) {
        this.clickHouseClient = clickHouseClient;
        this.configuration = configuration;
        this.aggregateServiceHandler = aggregateServiceHandler;
    }

    public void handle(BaseData baseData) {
        eventBus.postAsync(baseData);
    }

    @Override
    public void start() throws Exception {
        DataConfig dataConfig = configuration.getSystemConfig().getData();
        EventBusBuilder eventBusBuilder = EventBus.newEventBusBuilder()
                .queueSize(dataConfig.getDataInsertQueue())
                .threadName("base-data-persistence-server")
                .threadSize(dataConfig.getDataInsertThreadSize())
                .submitEventWaitTime(dataConfig.getDataInsertWaitTimeMills())
                .defaultSubscription((Subscription) new DefaultBaseDataPersistenceHandler(configuration, aggregateServiceHandler, clickHouseClient));

        List<BaseDataDefinition> baseDataDefinitions = configuration.getBaseDataDefinitions();
        if (null == baseDataDefinitions || baseDataDefinitions.isEmpty()) {
            return;
        }

        List<Subscription<Event<?>>> subscriptions = new ArrayList<>(baseDataDefinitions.size());
        for (BaseDataDefinition baseDataDefinition : configuration.getBaseDataDefinitions()) {
            if (null != baseDataDefinition.getPersistenceHandler() && !"".equals(baseDataDefinition.getPersistenceHandler())) {
                BaseDataPersistenceHandler persistenceHandler = null;
                try {
                    persistenceHandler = (BaseDataPersistenceHandler) Class.forName(baseDataDefinition.getPersistenceHandler())
                            .getConstructor(Configuration.class, AggregateServiceHandler.class, ClickHouseClient.class)
                            .newInstance(configuration, aggregateServiceHandler, clickHouseClient);
                    subscriptions.add(persistenceHandler);
                } catch (Exception e) {
                    if (null != persistenceHandler) {
                        logger.error("create base data persistence handler[" + persistenceHandler.id() + "] error", e);
                    } else {
                        logger.error("create base data persistence handler[" + baseDataDefinition.getPersistenceHandler() + "] error", e);
                    }
                }
            }
        }
        eventBusBuilder.subscription(subscriptions);
        this.eventBus = eventBusBuilder.build(configuration);
    }

    @Override
    public void stop() throws Exception {
        if (this.eventBus != null)
            this.eventBus.stop();
    }
}
