package cn.berry.rapids.aggregate;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.aggregate.calculation.CalculationHandler;
import cn.berry.rapids.aggregate.consistency.AggregateBlockPersistenceHandler;
import cn.berry.rapids.clickhouse.meta.ClickHouseMetaConfiguration;
import cn.berry.rapids.configuration.AggregateConfig;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.*;
import com.berry.clickhouse.tcp.client.ClickHouseClient;
import com.berry.clickhouse.tcp.client.data.Block;

import java.util.ArrayList;
import java.util.List;

public class AggregateServer implements AggregateServiceHandler, CycleLife {

    private final Configuration configuration;

    private final AggregateBlockPersistenceHandler persistenceHandler;

    private List<Subscription<Event<?>>> subscriptions;

    private EventBus eventBus;

    private final long waitTimeMills;

    public AggregateServer(Configuration configuration, ClickHouseClient clickHouseClient) {
        this.configuration = configuration;
        this.waitTimeMills = configuration.getSystemConfig().getAggregate().getAggregateWaitTime();
        this.persistenceHandler = new AggregateBlockPersistenceHandler(clickHouseClient, configuration);
        createCalculationHandler(persistenceHandler);
    }

    @Override
    public void handle(BlockEvent blockEvent) {
        this.eventBus.postAsync(blockEvent, this.waitTimeMills);
    }

    private void createCalculationHandler(AggregateBlockPersistenceHandler persistenceHandler) {
        ClickHouseMetaConfiguration metaConfiguration = configuration.getClickHouseMetaConfiguration();
        List<ClickHouseMetaConfiguration.Meta> metas = metaConfiguration.getMetaData().getMetas();

        this.subscriptions = new ArrayList<>(metas.size());
        for (ClickHouseMetaConfiguration.Meta meta : metas) {
            if (null != meta.getCalculationClass() && !"".equals(meta.getCalculationClass())) {
                try {
                    CalculationHandler calculationHandler = (CalculationHandler) Class.forName(meta.getCalculationClass())
                            .getConstructor(String.class, Configuration.class)
                            .newInstance(meta.getName(), configuration);
                    subscriptions.add(transToEvent(new Subscription<>() {
                        @Override
                        public String id() {
                            return calculationHandler.id();
                        }

                        @Override
                        public String type() {
                            return calculationHandler.type();
                        }

                        @Override
                        public void onMessage(BlockEvent event) {
                            if (!event.hasMessage()) {
                                return;
                            }
                            Block block = calculationHandler.handle(event);
                            if (null != block) {
                                persistenceHandler.handle(new BlockEvent(event.type(), block));
                            }
                        }
                    }));
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
        EventBusBuilder eventBusBuilder = EventBus.newEventBusBuilder()
                .queueSize(aggregateConfig.getAggregateWaitQueue())
                .threadSize(aggregateConfig.getAggregateThreadSize())
                .subscription(this.subscriptions);
        this.eventBus = eventBusBuilder.build(configuration);
    }

    private Subscription transToEvent(Subscription<BlockEvent> subscription) {
        return subscription;
    }

    @Override
    public void stop() throws Exception {
        this.persistenceHandler.stop();
        this.eventBus.stop();
    }
}
