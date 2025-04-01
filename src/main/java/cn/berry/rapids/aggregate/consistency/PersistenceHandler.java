package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.eventbus.BlockEvent;
import cn.berry.rapids.eventbus.Subscription;

public interface PersistenceHandler extends CycleLife, Subscription<BlockEvent> {

    boolean handle(BlockEvent entry);

    @Override
    default void onMessage(BlockEvent event) {
        handle(event);
    }
}
