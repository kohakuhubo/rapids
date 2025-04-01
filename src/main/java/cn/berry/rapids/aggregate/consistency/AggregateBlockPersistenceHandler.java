package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.Stoppable;
import cn.berry.rapids.aggregate.consistency.impl.AbstractPersistenceHandler;
import cn.berry.rapids.clickhouse.meta.ClickHouseMetaConfiguration;
import cn.berry.rapids.configuration.AggregateConfig;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.*;
import com.berry.clickhouse.tcp.client.ClickHouseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AggregateBlockPersistenceHandler extends Stoppable implements CycleLife {

    private static final Logger logger = LoggerFactory.getLogger(AggregateBlockPersistenceHandler.class);

    private final Configuration configuration;

    private final List<PersistenceHandlerDefinition> persistenceHandlers = new ArrayList<>();

    private final ClickHouseClient clickHouseClient;

    private static final String TYPE = "persistence-handler";

    private EventBus eventBus;

    private List<Subscription<Event<?>>> subscriptions;

    public AggregateBlockPersistenceHandler(ClickHouseClient clickHouseClient, Configuration configuration) {
        this.configuration = configuration;
        this.clickHouseClient = clickHouseClient;
    }

    private void createPersistenceHandler() {
        ClickHouseMetaConfiguration metaConfiguration = configuration.getClickHouseMetaConfiguration();
        List<ClickHouseMetaConfiguration.Meta> metas = metaConfiguration.getMetaData().getMetas();

        this.subscriptions = new ArrayList<>(metas.size());
        for (ClickHouseMetaConfiguration.Meta meta : metas) {
            if (null != meta.getPersistenceHandler() && !"".equals(meta.getPersistenceHandler())) {
                PersistenceHandler persistenceHandler = null;
                try {
                    persistenceHandler = (PersistenceHandler) Class.forName(meta.getPersistenceHandler())
                            .getDeclaredConstructor().newInstance();
                    if (persistenceHandler instanceof AbstractPersistenceHandler) {
                        ((AbstractPersistenceHandler) persistenceHandler).setClient(this.clickHouseClient);
                    }
                    persistenceHandler.start();
                    persistenceHandlers.add(new PersistenceHandlerDefinition(persistenceHandler));
                    subscriptions.add((Subscription) persistenceHandler);
                } catch (Exception e) {
                    if (null != persistenceHandler) {
                        logger.error("create persistence handler[{}] error", persistenceHandler.id(), e);
                    } else {
                        logger.error("create persistence handler[{}] error", meta.getPersistenceHandler(), e);
                    }
                }
            }
        }
    }

    public boolean handle(BlockEvent entry) {
        if (null == entry || isTerminal())
            return false;
        eventBus.postAsync(entry);
        return true;
    }

    @Override
    public void start() throws Exception {
        createPersistenceHandler();
        AggregateConfig aggregateConfig = configuration.getSystemConfig().getAggregate();
        EventBusBuilder eventBusBuilder = EventBus.newEventBusBuilder().eventType(TYPE)
                .queueSize(aggregateConfig.getAggregateInsertQueue())
                .threadSize(aggregateConfig.getInsertThreadSize())
                .subscription(subscriptions)
                .defaultSubscription((Subscription) new DefaultPersistenceHandler(TYPE, clickHouseClient, this.configuration));
        this.eventBus = eventBusBuilder.build(configuration);
    }

    @Override
    public void stop() {
        super.stop();
        eventBus.stop();
        for (PersistenceHandlerDefinition definition : persistenceHandlers) {
            PersistenceHandler persistenceHandler = definition.persistenceHandler();
            try {
                persistenceHandler.stop();
            } catch (Throwable e) {
                logger.error("persistence handler[{}] stop error", persistenceHandler.id(), e);
            }
        }
    }

    private record PersistenceHandlerDefinition(PersistenceHandler persistenceHandler) {
    }
}


