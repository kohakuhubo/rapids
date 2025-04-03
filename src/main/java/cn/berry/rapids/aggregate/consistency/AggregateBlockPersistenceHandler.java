package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.Stoppable;
import cn.berry.rapids.aggregate.consistency.impl.AbstractAggregatePersistenceHandler;
import cn.berry.rapids.clickhouse.meta.ClickHouseMetaConfiguration;
import cn.berry.rapids.configuration.AggregateConfig;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.*;
import com.berry.clickhouse.tcp.client.ClickHouseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 聚合块持久化处理器
 * 
 * 描述: 负责将聚合块持久化到存储中。
 * 此类实现了生命周期接口，管理持久化处理器的启动和停止。
 * 
 * 特性:
 * 1. 支持多种持久化策略
 * 2. 处理聚合块的存储
 * 
 * @author Berry
 * @version 1.0.0
 */
public class AggregateBlockPersistenceHandler extends Stoppable implements CycleLife {

    private static final Logger logger = LoggerFactory.getLogger(AggregateBlockPersistenceHandler.class);

    private final Configuration configuration;

    private final List<RunningAggregatePersistenceHandler> persistenceHandlers = new ArrayList<>();

    private final ClickHouseClient clickHouseClient;

    private EventBus eventBus;

    private List<Subscription<Event<?>>> subscriptions;

    /**
     * 构造聚合块持久化处理器
     * 
     * @param clickHouseClient ClickHouse客户端
     * @param configuration 应用配置对象
     */
    public AggregateBlockPersistenceHandler(ClickHouseClient clickHouseClient, Configuration configuration) {
        this.configuration = configuration;
        this.clickHouseClient = clickHouseClient;
    }

    /**
     * 创建持久化处理器
     */
    private void createPersistenceHandler() {
        ClickHouseMetaConfiguration metaConfiguration = configuration.getClickHouseMetaConfiguration();
        List<ClickHouseMetaConfiguration.Meta> metas = metaConfiguration.metaData().getMetas();

        this.subscriptions = new ArrayList<>(metas.size());
        for (ClickHouseMetaConfiguration.Meta meta : metas) {
            if (null != meta.getPersistenceHandler() && !"".equals(meta.getPersistenceHandler())) {
                AggregatePersistenceHandler aggregatePersistenceHandler;
                try {
                    aggregatePersistenceHandler = (AggregatePersistenceHandler) Class.forName(meta.getPersistenceHandler())
                            .getDeclaredConstructor().newInstance();
                    if (aggregatePersistenceHandler instanceof AbstractAggregatePersistenceHandler abstractAggregatePersistenceHandler) {
                        abstractAggregatePersistenceHandler.setClient(this.clickHouseClient);
                        abstractAggregatePersistenceHandler.start();
                    }

                    RunningAggregatePersistenceHandler runningHandler = new RunningAggregatePersistenceHandler(meta.getSourceType(), aggregatePersistenceHandler);
                    persistenceHandlers.add(runningHandler);
                    subscriptions.add((Subscription) runningHandler);
                } catch (Exception e) {
                    logger.error("create persistence handler[{}] error", meta.getPersistenceHandler(), e);
                }
            }
        }
    }

    /**
     * 处理区块事件
     * 
     * @param entry 区块事件
     * @return 是否处理成功
     */
    public boolean handle(BlockDataEvent entry) {
        if (null == entry || isTerminal())
            return false;
        eventBus.postAsync(entry);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() throws Exception {
        createPersistenceHandler();
        AggregateConfig aggregateConfig = configuration.getSystemConfig().getAggregate();
        EventBusBuilder eventBusBuilder = EventBus.newEventBusBuilder()
                .queueSize(aggregateConfig.getAggregateInsertQueue())
                .threadSize(aggregateConfig.getInsertThreadSize())
                .subscription(subscriptions)
                .defaultSubscription((Subscription) new RunningAggregatePersistenceHandler(null, new DefaultAggregatePersistenceHandler(clickHouseClient, this.configuration)));
        this.eventBus = eventBusBuilder.build(configuration);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop() {
        super.stop();
        eventBus.stop();
        for (RunningAggregatePersistenceHandler runningHandler : persistenceHandlers) {
            if (runningHandler.getAggregatePersistenceHandler() instanceof CycleLife cycleLife) {
                try {
                    cycleLife.stop();
                } catch (Throwable e) {
                    logger.error("persistence handler[" + cycleLife.getClass().getSimpleName() + "] stop error", e);
                }
            }
        }
    }
}


