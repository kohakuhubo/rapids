package cn.berry.rapids.data.persistece;

import cn.berry.rapids.eventbus.Event;
import cn.berry.rapids.eventbus.Subscription;
import cn.berry.rapids.model.SourceDataEvent;

public class RunningSourceDataPersistenceHandler implements Subscription<Event<SourceDataEvent>>  {

    private final String type;

    private final SourceDataPersistenceHandler sourceDataPersistenceHandler;

    public RunningSourceDataPersistenceHandler(String type, SourceDataPersistenceHandler sourceDataPersistenceHandler) {
        this.type = type;
        this.sourceDataPersistenceHandler = sourceDataPersistenceHandler;
    }

    @Override
    public String type() {
        return this.type;
    }

    @Override
    public void onMessage(Event<SourceDataEvent> event) {
        sourceDataPersistenceHandler.handle(event.getMessage());
    }

    public SourceDataPersistenceHandler getSourceDataPersistenceHandler() {
        return sourceDataPersistenceHandler;
    }
}
