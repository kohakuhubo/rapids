package cn.berry.rapids.aggregate;

import cn.berry.rapids.eventbus.Event;

public interface AggregateEntry extends Event<AggregateEntry> {

    String type();

    long startRowId();

    long endRowId();

    int size();

    @Override
    default AggregateEntry getMessage() {
        return this;
    }
}
