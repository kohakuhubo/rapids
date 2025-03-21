package cn.berry.rapids;

import cn.berry.rapids.aggregate.AggregateEntry;

import java.util.ArrayList;
import java.util.List;

public class AggregateView extends AbstractAggregateEntry {

    private final List<AggregateEntry> entries = new ArrayList<>();


    public AggregateView(long startRowId, long endRowId) {
        super(startRowId, endRowId);
    }

    public void addEntry(AggregateEntry entry) {
        entries.add(entry);
    }

    public List<AggregateEntry> getEntries() {
        return entries;
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public String type() {
        return "view";
    }
}
