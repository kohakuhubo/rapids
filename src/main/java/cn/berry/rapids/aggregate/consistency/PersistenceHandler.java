package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.aggregate.AggregateEntry;

public interface PersistenceHandler extends CycleLife {

    String id();

    void id(String id);

    boolean handle(AggregateEntry entry);

}
