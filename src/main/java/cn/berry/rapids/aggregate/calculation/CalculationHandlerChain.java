package cn.berry.rapids.aggregate.calculation;

import cn.berry.rapids.AggregateView;
import cn.berry.rapids.aggregate.AggregateEntry;
import cn.berry.rapids.aggregate.DataWrapper;

import java.util.Set;

public class CalculationHandlerChain {

    private final CalculationHandler handler;

    private final CalculationHandlerChain chain;

    public CalculationHandlerChain(CalculationHandler handler) {
        this(handler, null);
    }

    public CalculationHandlerChain(CalculationHandler handler, CalculationHandlerChain chain) {
        this.handler = handler;
        this.chain = chain;
    }

    public boolean handle(DataWrapper dataWrapper, AggregateView view) {
        Set<String> aggregateTypes = dataWrapper.getState().getAggregateTypes();
        if (aggregateTypes.contains("all") || aggregateTypes.contains(handler.type())) {
            AggregateEntry aggregateEntry = handler.handle(dataWrapper);
            if (null != aggregateEntry && aggregateEntry.size() > 0) {
                view.addEntry(aggregateEntry);
            }
        }

        if (null != chain)
            return chain.handle(dataWrapper, view);
        return true;
    }
}
