package cn.berry.rapids.aggregate;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.aggregate.calculation.CalculationHandler;
import cn.berry.rapids.aggregate.calculation.CalculationHandlerChain;
import cn.berry.rapids.aggregate.consistency.AggregateBlockPersistenceHandler;
import cn.berry.rapids.clickhouse.meta.ClickHouseMetaConfiguration;
import cn.berry.rapids.configuration.AggregateConfig;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.Event;
import cn.berry.rapids.eventbus.EventBus;
import cn.berry.rapids.eventbus.EventBusBuilder;
import cn.berry.rapids.eventbus.Subscription;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregateServer implements AggregateServiceHandler, CycleLife {

    private final Configuration configuration;

    private final AggregateBlockPersistenceHandler persistenceHandler;

    private final Map<String, CalculationHandler> calculationHandlersMap = new HashMap<>();

    private final Map<String, CalculationHandler> typeCalculationHandlersMap = new HashMap<>();

    private EventBus eventBus;

    private final long waitTimeMills;

    public AggregateServer(Configuration configuration, ClickHouseClient clickHouseClient) {
        this.configuration = configuration;
        this.waitTimeMills = configuration.getSystemConfig().getAggregate().getAggregateWaitTime();
        this.persistenceHandler = new AggregateBlockPersistenceHandler(clickHouseClient, configuration);
        createCalculationHandler(persistenceHandler);
    }

    @Override
    public void handle(DataWrapper dataWrapper) {
        this.eventBus.postAsync(dataWrapper, this.waitTimeMills);
    }

    private void createCalculationHandler(AggregateBlockPersistenceHandler persistenceHandler) {
        ClickHouseMetaConfiguration metaConfiguration = configuration.getClickHouseMetaConfiguration();
        List<ClickHouseMetaConfiguration.Meta> metas = metaConfiguration.getMetaData().getMetas();
        Map<String, CalculationHandlerChain> tmp = new HashMap<>(metas.size());

        for (ClickHouseMetaConfiguration.Meta meta : metaConfiguration.getMetaData().getMetas()) {
            if (null != meta.getCalculationClass() && !"".equals(meta.getCalculationClass())) {

                CalculationHandlerChain preChain = null;
                if (tmp.containsKey(meta.getSourceType())) {
                    preChain = tmp.get(meta.getSourceType());
                }
                try {
                    CalculationHandler calculationHandler = (CalculationHandler) Class.forName(meta.getCalculationClass())
                            .getConstructor(String.class, Configuration.class)
                            .newInstance(meta.getName(), configuration);
                    this.typeCalculationHandlersMap.putIfAbsent(calculationHandler.id(), calculationHandler);
                    if (null == preChain) {
                        preChain = new CalculationHandlerChain(persistenceHandler, calculationHandler);
                        this.calculationHandlersMap.putIfAbsent(calculationHandler.type(), calculationHandler);
                    } else {
                        preChain = new CalculationHandlerChain(persistenceHandler, calculationHandler, preChain);
                    }
                    tmp.put(meta.getSourceType(), preChain);
                } catch (Exception e) {
                    //ignore
                }
            }
        }
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
