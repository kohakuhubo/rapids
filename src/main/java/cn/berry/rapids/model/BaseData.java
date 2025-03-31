package cn.berry.rapids.model;

import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.eventbus.Event;
import com.berry.clickhouse.tcp.client.data.Block;

public class BaseData implements Event<BaseData> {

    private final int maxRowCnt;

    private final long maxBlockSize;

    private SourceEntry<?> sourceEntry;

    private final Block block;

    public BaseData(int maxRowCnt, long maxBlockSize, Block block) {
        this.maxRowCnt = maxRowCnt;
        this.maxBlockSize = maxBlockSize;
        this.block = block;
    }

    public BaseData(int maxRowCnt, long maxBlockSize, Block block, SourceEntry<?> sourceEntry) {
        this.maxRowCnt = maxRowCnt;
        this.maxBlockSize = maxBlockSize;
        this.sourceEntry = sourceEntry;
        this.block = block;
    }

    public void setSourceEntry(SourceEntry<?> sourceEntry) {
        this.sourceEntry = sourceEntry;
    }

    public Block getBlock() {
        return block;
    }

    public boolean isEmpty() {
        return block.rowCnt() == 0;
    }

    public boolean isFull() {
        return block.rowCnt() >= maxRowCnt;
    }

    public SourceEntry<?> getSourceEntry() {
        return sourceEntry;
    }

    @Override
    public String type() {
        return "base";
    }

    @Override
    public BaseData getMessage() {
        return this;
    }
}
