package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.AggregateView;
import cn.berry.rapids.CycleLife;
import cn.berry.rapids.Stoppable;
import cn.berry.rapids.aggregate.AggregateEntry;
import cn.berry.rapids.aggregate.calculation.CalculationHandler;
import cn.berry.rapids.clickhouse.meta.ClickHouseMetaConfiguration;
import cn.berry.rapids.configuration.AggregateConfig;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.*;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregateBlockPersistenceHandler extends Stoppable implements CycleLife {

    private final Configuration configuration;

    private final List<PersistenceHandlerDefinition> persistenceHandlers = new ArrayList<>();

    private final ClickHouseClient clickHouseClient;

    private static final String TYPE = "persistence-handler";

    private EventBus eventBus;

    public AggregateBlockPersistenceHandler(ClickHouseClient clickHouseClient, Configuration configuration) {
        this.configuration = configuration;
        this.clickHouseClient = clickHouseClient;
    }

    public boolean handle(BlockEvent entry) {
        if (null == entry || isTerminal())
            return false;
        eventBus.postAsync(entry);
        return true;
    }

    @Override
    public void start() throws Exception {
        AggregateConfig aggregateConfig = configuration.getSystemConfig().getAggregate();

        EventBusBuilder eventBusBuilder = EventBus.newEventBusBuilder().eventType(TYPE)
                .queueSize(aggregateConfig.getAggregateInsertQueue());
        for (int i = 0; i < aggregateConfig.getInsertThreadSize(); i++) {
            PersistenceHandler persistenceHandler = new DefaultPersistenceHandler(TYPE + "-" + i, clickHouseClient, this.configuration);
            persistenceHandlers.add(new PersistenceHandlerDefinition(persistenceHandler));
            eventBusBuilder.subscription((Subscription) persistenceHandler);
        }
        this.eventBus = eventBusBuilder.build(configuration);
    }

    @Override
    public void stop() {
        super.stop();
        eventBus.stop();
        for (PersistenceHandlerDefinition definition : persistenceHandlers) {
            try {
                definition.persistenceHandler().stop();
            } catch (Throwable e) {

            }
        }
    }

    private record PersistenceHandlerDefinition(PersistenceHandler persistenceHandler) {
    }
}


