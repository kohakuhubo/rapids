package cn.berry.rapids.aggregate.calculation;

import cn.berry.rapids.aggregate.consistency.AggregateBlockPersistenceHandler;
import cn.berry.rapids.eventbus.BlockEvent;
import com.berry.clickhouse.tcp.client.data.Block;
import org.apache.commons.lang3.StringUtils;

/**
 * 计算处理器链
 * 
 * 描述: 管理多个计算处理器的链，依次处理区块事件。
 * 
 * 特性:
 * 1. 支持链式调用多个处理器
 * 2. 处理器可以是聚合计算处理器
 * 
 * @author Berry
 * @version 1.0.0
 */
public class CalculationHandlerChain {

    private final CalculationHandler handler;

    private final CalculationHandlerChain chain;

    private final AggregateBlockPersistenceHandler persistenceHandler;

    /**
     * 构造函数
     * 
     * @param persistenceHandler 数据持久化处理器
     * @param handler 计算处理器
     */
    public CalculationHandlerChain(AggregateBlockPersistenceHandler persistenceHandler, CalculationHandler handler) {
        this(persistenceHandler, handler, null);
    }

    /**
     * 构造函数
     * 
     * @param persistenceHandler 数据持久化处理器
     * @param handler 计算处理器
     * @param chain 下一个处理器链
     */
    public CalculationHandlerChain(AggregateBlockPersistenceHandler persistenceHandler, CalculationHandler handler, CalculationHandlerChain chain) {
        this.persistenceHandler = persistenceHandler;
        this.handler = handler;
        this.chain = chain;
    }

    /**
     * 处理区块事件
     * 
     * @param event 区块事件
     * @return 是否处理成功
     */
    public boolean handle(BlockEvent event) {
        if (StringUtils.equalsAnyIgnoreCase(handler.type(), event.type())) {
            Block block = handler.handle(event);
            if (null != block) {
                persistenceHandler.handle(new BlockEvent(handler.type(), block));
            }
        }
        if (null != chain)
            return chain.handle(event);
        return true;
    }
}
