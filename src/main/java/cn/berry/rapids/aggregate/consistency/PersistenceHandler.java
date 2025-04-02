package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.eventbus.BlockEvent;
import cn.berry.rapids.eventbus.Subscription;

/**
 * 持久化处理器接口
 * 
 * 描述: 定义持久化处理的基本方法。
 * 实现此接口的类负责处理区块事件的持久化。
 * 
 * @author Berry
 * @version 1.0.0
 */
public interface PersistenceHandler extends CycleLife, Subscription<BlockEvent> {

    /**
     * 处理区块事件
     * 
     * @param entry 区块事件
     * @return 是否处理成功
     */
    boolean handle(BlockEvent entry);

    @Override
    default void onMessage(BlockEvent event) {
        handle(event);
    }
}
