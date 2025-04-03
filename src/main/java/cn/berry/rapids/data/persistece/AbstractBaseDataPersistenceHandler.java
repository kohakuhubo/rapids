package cn.berry.rapids.data.persistece;

import cn.berry.rapids.Stoppable;
import cn.berry.rapids.aggregate.AggregateServiceHandler;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.Event;
import cn.berry.rapids.model.BaseData;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

public abstract class AbstractBaseDataPersistenceHandler extends Stoppable implements BaseDataPersistenceHandler<BaseData> {

    private final Configuration configuration;

    private final AggregateServiceHandler aggregateServiceHandler;

    private final ClickHouseClient clickHouseClient;

    protected abstract void flush();

    public AbstractBaseDataPersistenceHandler(Configuration configuration, AggregateServiceHandler aggregateServiceHandler, ClickHouseClient clickHouseClient) {
        this.configuration = configuration;
        this.aggregateServiceHandler = aggregateServiceHandler;
        this.clickHouseClient = clickHouseClient;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public AggregateServiceHandler getAggregateServiceHandler() {
        return aggregateServiceHandler;
    }

    public ClickHouseClient getClickHouseClient() {
        return clickHouseClient;
    }

    @Override
    public void onMessage(Event<BaseData> event) {
        if (isTerminal()) {
            return;
        }

        if (event.hasMessage()) {
            handle(event.getMessage());
        } else {
            flush();
        }
    }
}
