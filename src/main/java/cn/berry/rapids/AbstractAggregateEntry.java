package cn.berry.rapids;

import cn.berry.rapids.aggregate.AggregateEntry;

public abstract class AbstractAggregateEntry implements AggregateEntry {

    private final long startRowId;

    private final long endRowId;

    public AbstractAggregateEntry(long startRowId, long endRowId) {
        this.startRowId = startRowId;
        this.endRowId = endRowId;
    }

    @Override
    public long startRowId() {
        return this.startRowId;
    }

    @Override
    public long endRowId() {
        return this.endRowId;
    }
}
