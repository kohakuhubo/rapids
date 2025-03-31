package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.aggregate.AggregateEntry;
import cn.berry.rapids.eventbus.BlockEvent;

public interface PersistenceHandler extends CycleLife {

    boolean handle(BlockEvent entry);

}
