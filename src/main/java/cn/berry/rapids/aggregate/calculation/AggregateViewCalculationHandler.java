package cn.berry.rapids.aggregate.calculation;

import cn.berry.rapids.AggregateView;
import cn.berry.rapids.aggregate.AggregateEntry;
import cn.berry.rapids.aggregate.DataWrapper;
import cn.berry.rapids.aggregate.consistency.PersistenceHandler;
import cn.berry.rapids.eventbus.Event;
import cn.berry.rapids.eventbus.Subscription;

public class AggregateViewCalculationHandler implements Subscription<Event<?>>, CalculationHandler {

    private final PersistenceHandler persistenceHandler;

    private final CalculationHandlerChain calculationHandlerChain;

    private String aggregateId;

    public AggregateViewCalculationHandler(PersistenceHandler persistenceHandler, CalculationHandlerChain calculationHandlerChain, String aggregateId) {
        this.persistenceHandler = persistenceHandler;
        this.calculationHandlerChain = calculationHandlerChain;
        this.aggregateId = aggregateId;
    }

    @Override
    public AggregateEntry handle(DataWrapper dataWrapper) {
        AggregateView view = new AggregateView(dataWrapper.getBatchData().startRowId(),
                dataWrapper.getBatchData().endRowId());
        try {
            calculationHandlerChain.handle(dataWrapper, view);
        } catch (Exception e) {
            //ignore
        } finally {
            dataWrapper.clean();
        }
        return null;
    }

    @Override
    public String id() {
        return this.aggregateId;
    }

    @Override
    public String type() {
        return this.aggregateId;
    }

    @Override
    public void onMessage(Event<?> event) {
        if (!event.hasMessage())
            return;
        DataWrapper dataWrapper = (DataWrapper) event.getMessage();
        AggregateEntry entry = handle(dataWrapper);
        if (null != entry) {
            persistenceHandler.handle(entry);
        }
    }
}
