package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.AggregateView;
import cn.berry.rapids.aggregate.AggregateEntry;
import cn.berry.rapids.aggregate.calculation.CalculationHandler;
import cn.berry.rapids.clickhouse.meta.ClickHouseMetaConfiguration;
import cn.berry.rapids.configuration.AggregateConfig;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.Event;
import cn.berry.rapids.eventbus.EventBusBuilder;
import cn.berry.rapids.eventbus.EventBus;
import cn.berry.rapids.eventbus.Subscription;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregateViewPersistenceHandler implements PersistenceHandler {

    private final Configuration configuration;

    private final Map<String, CalculationHandler> calculationHandlers = new HashMap<>();

    private final Map<String, EventBus> eventBusMap = new HashMap<>();

    private final Map<String, PersistenceHandlerDefinition> persistenceHandlers = new HashMap<>();

    private volatile boolean running = true;

    public AggregateViewPersistenceHandler(Configuration configuration) {
        this.configuration = configuration;
        createConsistencyHandler();
    }

    @Override
    public String id() {
        return "aggregate_view";
    }

    @Override
    public void id(String id) {

    }

    @Override
    public boolean handle(AggregateEntry entry) {
        List<AggregateEntry> localEntries = ((AggregateView) entry).getEntries();
        int total = localEntries.size();
        while (running && !Thread.currentThread().isInterrupted()) {
            for (int i = 0; i < localEntries.size(); i++) {
                AggregateEntry localEntry = localEntries.get(i);
                if (null != localEntry && localEntry.size() > 0) {
                    EventBus eventBus = eventBusMap.get(localEntry.type());
                    if (eventBus.postAsync(localEntry)) {
                        localEntries.set(i, null);
                        total--;
                    }
                } else {
                    localEntries.set(i, null);
                    total--;
                }
            }

            if (!running || total <= 0) {
                break;
            } else {
                try {
                    Thread.sleep(2000L);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
        return true;
    }

    @Override
    public void start() throws Exception {
        AggregateConfig aggregateConfig = configuration.getSystemConfig().getAggregate();

        Map<String, EventBusBuilder> busBuilderMap = new HashMap<>();
        for (PersistenceHandlerDefinition definition : persistenceHandlers.values()) {
            PersistenceHandler handler = definition.persistenceHandler();
            handler.start();
            Subscription<Event<?>> subscription = (Subscription<Event<?>>) handler;
            EventBusBuilder eventBusBuilder = busBuilderMap.get(subscription.type());
            if (null == eventBusBuilder) {
                eventBusBuilder = EventBus.newEventBusBuilder().eventType(subscription.type())
                        .queueSize(aggregateConfig.getAggregateInsertQueue());
                busBuilderMap.put(subscription.type(), eventBusBuilder);
            }
            eventBusBuilder.subscription(subscription);
        }
        for (EventBusBuilder eventBusBuilder : busBuilderMap.values()) {
            EventBus multipleEventBus = eventBusBuilder.build(configuration);
            eventBusMap.putIfAbsent(multipleEventBus.getEventType(), multipleEventBus);
        }
    }

    @Override
    public void stop() throws Exception {
        running = false;
        for (EventBus eventBus : eventBusMap.values())
            eventBus.stop();
        for (PersistenceHandlerDefinition definition : persistenceHandlers.values())
            definition.persistenceHandler().stop();
    }

    private void createConsistencyHandler() {
        ClickHouseMetaConfiguration metaConfiguration = configuration.getClickHouseMetaConfiguration();
        for (ClickHouseMetaConfiguration.Meta meta : metaConfiguration.getMetaData().getMetas()) {
            if (null != meta.getCalculationClass() && !"".equals(meta.getCalculationClass())) {
                if (null != meta.getThreadSize() && meta.getThreadSize() > 1) {
                    for (int i = 0; i < meta.getThreadSize(); i++) {
                        try {
                            PersistenceHandler persistenceHandler = (PersistenceHandler) Class.forName(meta.getConsistencyClass())
                                    .getConstructor(String.class, Configuration.class)
                                    .newInstance(meta.getName() + "_" + i, configuration);
                            persistenceHandlers.putIfAbsent(persistenceHandler.id(), new PersistenceHandlerDefinition(persistenceHandler, meta.getInsertQueueSize()));
                        } catch (Exception e) {
                            //ignore
                        }
                    }
                } else {
                    try {
                        PersistenceHandler persistenceHandler = (PersistenceHandler) Class.forName(meta.getConsistencyClass())
                                .getConstructor(String.class, Configuration.class)
                                .newInstance(meta.getName(), configuration);
                        persistenceHandlers.putIfAbsent(persistenceHandler.id(), new PersistenceHandlerDefinition(persistenceHandler, meta.getInsertQueueSize()));
                    } catch (Exception e) {
                        //ignore
                    }
                }
            }
        }
    }

    private record PersistenceHandlerDefinition(PersistenceHandler persistenceHandler, Integer queueSize) {
    }
}
