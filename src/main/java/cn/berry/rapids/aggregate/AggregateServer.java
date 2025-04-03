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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AggregateServer implements AggregateServiceHandler, CycleLife {

    /**
     * 日志记录器
     */
    private static final Logger logger = LoggerFactory.getLogger(AggregateServer.class);

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
    public void handle(BlockDataEvent blockDataEvent) {
        this.eventBus.postAsync(blockDataEvent, this.waitTimeMills);
    }

    private void createCalculationHandler(AggregateBlockPersistenceHandler persistenceHandler) {
        ClickHouseMetaConfiguration metaConfiguration = configuration.getClickHouseMetaConfiguration();
        List<ClickHouseMetaConfiguration.Meta> metas = metaConfiguration.metaData().getMetas();

        this.subscriptions = new ArrayList<>(metas.size());
        for (ClickHouseMetaConfiguration.Meta meta : metas) {
            if (null != meta.getCalculationClass() && !"".equals(meta.getCalculationClass())) {
                try {
                    CalculationHandler calculationHandler = (CalculationHandler) Class.forName(meta.getCalculationClass())
                            .getConstructor(String.class, Configuration.class)
                            .newInstance(meta.getName(), configuration);
                    subscriptions.add(transToEvent(new Subscription<>() {

                        @Override
                        public String type() {
                            return calculationHandler.type();
                        }

                        @Override
                        public void onMessage(BlockDataEvent event) {
                            if (!event.hasMessage()) {
                                return;
                            }
                            Block block = calculationHandler.handle(event);
                            if (null != block) {
                                persistenceHandler.handle(new BlockDataEvent(event.type(), block));
                            }
                        }
                    }));
                } catch (Exception e) {
                    logger.error("create calculation handler error", e);
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

    private Subscription transToEvent(Subscription<BlockDataEvent> subscription) {
        return subscription;
    }

    @Override
    public void stop() throws Exception {
        this.persistenceHandler.stop();
        this.eventBus.stop();
    }
}
