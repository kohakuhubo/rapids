package cn.berry.rapids.aggregate.calculation;

import cn.berry.rapids.aggregate.consistency.AggregateBlockPersistenceHandler;
import cn.berry.rapids.eventbus.BlockEvent;
import com.berry.clickhouse.tcp.client.data.Block;
import org.apache.commons.lang3.StringUtils;

public class CalculationHandlerChain {

    private final CalculationHandler handler;

    private final CalculationHandlerChain chain;

    private final AggregateBlockPersistenceHandler persistenceHandler;

    public CalculationHandlerChain(AggregateBlockPersistenceHandler persistenceHandler, CalculationHandler handler) {
        this(persistenceHandler, handler, null);
    }

    public CalculationHandlerChain(AggregateBlockPersistenceHandler persistenceHandler, CalculationHandler handler, CalculationHandlerChain chain) {
        this.persistenceHandler = persistenceHandler;
        this.handler = handler;
        this.chain = chain;
    }

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
