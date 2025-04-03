package cn.berry.rapids.data.persistece;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.aggregate.AggregateServiceHandler;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.configuration.DataConfig;
import cn.berry.rapids.definition.SourceDataDefinition;
import cn.berry.rapids.eventbus.Event;
import cn.berry.rapids.eventbus.EventBus;
import cn.berry.rapids.eventbus.EventBusBuilder;
import cn.berry.rapids.eventbus.Subscription;
import cn.berry.rapids.model.SourceDataEvent;
import com.berry.clickhouse.tcp.client.ClickHouseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class SourceDataPersistenceServer implements CycleLife {

    /**
     * 日志记录器
     */
    private static final Logger logger = LoggerFactory.getLogger(SourceDataPersistenceServer.class);

    private final Configuration configuration;

    private EventBus eventBus;

    private final AggregateServiceHandler aggregateServiceHandler;

    private final ClickHouseClient clickHouseClient;

    private final List<RunningSourceDataPersistenceHandler> persistenceHandlers = new ArrayList<>();

    public SourceDataPersistenceServer(Configuration configuration, AggregateServiceHandler aggregateServiceHandler, ClickHouseClient clickHouseClient) {
        this.clickHouseClient = clickHouseClient;
        this.configuration = configuration;
        this.aggregateServiceHandler = aggregateServiceHandler;
    }

    public void handle(SourceDataEvent sourceDataEvent) {
        eventBus.postAsync(sourceDataEvent);
    }

    @Override
    public void start() throws Exception {
        DataConfig dataConfig = configuration.getSystemConfig().getData();
        EventBusBuilder eventBusBuilder = EventBus.newEventBusBuilder()
                .queueSize(dataConfig.getDataInsertQueue())
                .threadName("source-data-persistence-server")
                .threadSize(dataConfig.getDataInsertThreadSize())
                .submitEventWaitTime(dataConfig.getDataInsertWaitTimeMills())
                .defaultSubscription((Subscription) new RunningSourceDataPersistenceHandler(null, new DefaultSourceDataPersistenceHandler(configuration, aggregateServiceHandler, clickHouseClient)));

        List<SourceDataDefinition> sourceDataDefinitions = configuration.getSourceDataDefinitions();
        if (null == sourceDataDefinitions || sourceDataDefinitions.isEmpty()) {
            return;
        }

        List<Subscription<Event<?>>> subscriptions = new ArrayList<>(sourceDataDefinitions.size());
        for (SourceDataDefinition sourceDataDefinition : configuration.getSourceDataDefinitions()) {
            if (null != sourceDataDefinition.getPersistenceHandler() && !"".equals(sourceDataDefinition.getPersistenceHandler())) {
                SourceDataPersistenceHandler persistenceHandler;
                try {
                    persistenceHandler = (SourceDataPersistenceHandler) Class.forName(sourceDataDefinition.getPersistenceHandler())
                            .getDeclaredConstructor().newInstance();
                    if (persistenceHandler instanceof AbstractSourceDataPersistenceHandler abstractSourceDataPersistenceHandler) {
                        abstractSourceDataPersistenceHandler.setClickHouseClient(clickHouseClient);
                        abstractSourceDataPersistenceHandler.setAggregateServiceHandler(aggregateServiceHandler);
                        abstractSourceDataPersistenceHandler.setConfiguration(configuration);
                        abstractSourceDataPersistenceHandler.start();
                    }
                    RunningSourceDataPersistenceHandler runningHandler = new RunningSourceDataPersistenceHandler(sourceDataDefinition.getSourceName(), persistenceHandler);
                    persistenceHandlers.add(runningHandler);
                    subscriptions.add((Subscription) runningHandler);
                } catch (Exception e) {
                    logger.error("create source data persistence handler[" + sourceDataDefinition.getPersistenceHandler() + "] error", e);
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

        for (RunningSourceDataPersistenceHandler runningHandler : persistenceHandlers) {
            if (runningHandler.getSourceDataPersistenceHandler() instanceof CycleLife cycleLife) {
                try {
                    cycleLife.stop();
                } catch (Throwable e) {
                    logger.error("persistence handler[" + cycleLife.getClass().getSimpleName() + "] stop error", e);
                }
            }
        }
    }
}
