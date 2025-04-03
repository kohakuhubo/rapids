package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.eventbus.BlockDataEvent;
import cn.berry.rapids.eventbus.Subscription;

public class RunningAggregatePersistenceHandler implements Subscription<BlockDataEvent>  {

    private final String type;

    private final AggregatePersistenceHandler aggregatePersistenceHandler;

    public RunningAggregatePersistenceHandler(String type, AggregatePersistenceHandler aggregatePersistenceHandler) {
        this.type = type;
        this.aggregatePersistenceHandler = aggregatePersistenceHandler;
    }

    @Override
    public String type() {
        return this.type;
    }

    @Override
    public void onMessage(BlockDataEvent event) {
        aggregatePersistenceHandler.handle(event);
    }

    public AggregatePersistenceHandler getAggregatePersistenceHandler() {
        return aggregatePersistenceHandler;
    }
}
