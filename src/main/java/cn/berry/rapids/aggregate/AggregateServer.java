package cn.berry.rapids.aggregate;

import cn.berry.rapids.AppServer;
import cn.berry.rapids.CycleLife;
import cn.berry.rapids.aggregate.calculation.AggregateViewCalculationHandler;
import cn.berry.rapids.aggregate.calculation.CalculationHandler;
import cn.berry.rapids.aggregate.calculation.CalculationHandlerChain;
import cn.berry.rapids.aggregate.consistency.AggregateViewPersistenceHandler;
import cn.berry.rapids.clickhouse.meta.ClickHouseMetaConfiguration;
import cn.berry.rapids.configuration.AggregateConfig;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.Event;
import cn.berry.rapids.eventbus.EventBus;
import cn.berry.rapids.eventbus.EventBusBuilder;
import cn.berry.rapids.eventbus.Subscription;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

import java.util.HashMap;
import java.util.Map;

public class AggregateServer implements AggregateServiceHandler, CycleLife {

    private final AppServer appServer;

    private final Configuration configuration;

    private AggregateViewCalculationHandler calculationHandler;

    private final AggregateViewPersistenceHandler persistenceHandler;

    private final Map<String, CalculationHandler> calculationHandlersMap = new HashMap<>();

    private final Map<String, CalculationHandler> typeCalculationHandlersMap = new HashMap<>();

    private EventBus eventBus;

    private ClickHouseClient clickHouseClient;

    public AggregateServer(AppServer appServer, Configuration configuration, ClickHouseClient clickHouseClient) {
        this.appServer = appServer;
        this.configuration = configuration;
        this.persistenceHandler = new AggregateViewPersistenceHandler(configuration);
        this.clickHouseClient = clickHouseClient;

        AggregateConfig aggregateConfig = configuration.getSystemConfig().getAggregate();
        for (int i = 0; i < aggregateConfig.getAggregateThreadSize(); i++) {
            createCalculationHandler(persistenceHandler, i);
        }
    }
    @Override
    public void handle(DataWrapper dataWrapper) {
        this.eventBus.postAsync(dataWrapper, 500L);
    }

    private void createCalculationHandler(AggregateViewPersistenceHandler persistenceHandler, int num) {
        ClickHouseMetaConfiguration metaConfiguration = configuration.getClickHouseMetaConfiguration();
        CalculationHandlerChain preChain = null;
        for (ClickHouseMetaConfiguration.Meta meta : metaConfiguration.getMetaData().getMetas()) {
            if (null != meta.getCalculationClass() && !"".equals(meta.getCalculationClass())) {
                try {
                    CalculationHandler calculationHandler = (CalculationHandler) Class.forName(meta.getCalculationClass())
                            .getConstructor(String.class, Configuration.class)
                            .newInstance(meta.getName(), configuration);
                    if (null == preChain) {
                        preChain = new CalculationHandlerChain(calculationHandler);
                    } else {
                        preChain = new CalculationHandlerChain(calculationHandler, preChain);
                    }
                    typeCalculationHandlersMap.putIfAbsent(calculationHandler.type(), calculationHandler);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        this.calculationHandler = new AggregateViewCalculationHandler(persistenceHandler, preChain, "base-" + num);
        this.calculationHandlersMap.putIfAbsent(this.calculationHandler.id(), this.calculationHandler);
    }

    @Override
    public void start() throws Exception {
        this.persistenceHandler.start();
        AggregateConfig aggregateConfig = configuration.getSystemConfig().getAggregate();
        EventBusBuilder eventBusBuilder = EventBus.newEventBusBuilder().eventType("base")
                .queueSize(aggregateConfig.getAggregateWaitQueue());
        for (CalculationHandler calculationHandler : calculationHandlersMap.values()) {
            eventBusBuilder.subscription((Subscription<Event<?>>) calculationHandler);
        }
        this.eventBus = eventBusBuilder.build(configuration);
    }

    @Override
    public void stop() throws Exception {
        this.persistenceHandler.stop();
        this.eventBus.stop();
    }
}
