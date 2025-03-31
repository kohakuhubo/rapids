package cn.berry.rapids;

import cn.berry.rapids.aggregate.AggregateEntry;
import cn.berry.rapids.eventbus.BlockEvent;

import java.util.ArrayList;
import java.util.List;

public class AggregateView {

    private final List<BlockEvent> entries = new ArrayList<>();

    public void addEntry(BlockEvent entry) {
        entries.add(entry);
    }

    public List<BlockEvent> getEntries() {
        return entries;
    }
}
