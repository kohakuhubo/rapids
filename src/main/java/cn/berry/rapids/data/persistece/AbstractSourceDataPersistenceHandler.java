package cn.berry.rapids.data.persistece;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.Stoppable;
import cn.berry.rapids.aggregate.AggregateServiceHandler;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.model.SourceDataEvent;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

public abstract class AbstractSourceDataPersistenceHandler extends Stoppable implements SourceDataPersistenceHandler, CycleLife {

    private Configuration configuration;

    private AggregateServiceHandler aggregateServiceHandler;

    private ClickHouseClient clickHouseClient;

    protected abstract void flush();

    protected abstract void doHandle(SourceDataEvent sourceDataEvent);

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public void setAggregateServiceHandler(AggregateServiceHandler aggregateServiceHandler) {
        this.aggregateServiceHandler = aggregateServiceHandler;
    }

    public void setClickHouseClient(ClickHouseClient clickHouseClient) {
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
    public void handle(SourceDataEvent sourceDataEvent) {
        if (isTerminal()) {
            return;
        }

        if (null != sourceDataEvent) {
            doHandle(sourceDataEvent);
        } else {
            flush();
        }
    }
}
