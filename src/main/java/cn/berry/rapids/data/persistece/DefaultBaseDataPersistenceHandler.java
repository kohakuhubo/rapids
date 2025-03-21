package cn.berry.rapids.data.persistece;

import cn.berry.rapids.aggregate.AggregateServiceHandler;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.Event;
import cn.berry.rapids.eventbus.Subscription;
import cn.berry.rapids.model.BaseData;

public class DefaultBaseDataPersistenceHandler implements BaseDataPersistenceHandler<BaseData>, Subscription<Event<BaseData>> {

    private final Configuration configuration;

    private final AggregateServiceHandler aggregateServiceHandler;

    private final String id;

    public DefaultBaseDataPersistenceHandler(int num, Configuration configuration, AggregateServiceHandler aggregateServiceHandler) {
        this.id = "base-" + num;
        this.configuration = configuration;
        this.aggregateServiceHandler = aggregateServiceHandler;
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public String type() {
        return "base";
    }

    @Override
    public void onMessage(Event<BaseData> event) {
        handle(event.getMessage());
    }

    @Override
    public void handle(BaseData baseData) {

    }
}
