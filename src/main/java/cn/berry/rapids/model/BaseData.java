package cn.berry.rapids.model;

import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.eventbus.Event;
import com.berry.clickhouse.tcp.client.data.Block;

public class BaseData implements Event<BaseData> {

    private long startRowId;

    private long endRowId;

    private long dataStartTime;

    private long dataEndTime;

    private final int maxRowCnt;

    private final long maxBlockSize;

    private final SourceEntry<?> sourceEntry;

    private int rowCnt;

    private long blockSize;

    private Block block;

    public BaseData(int maxRowCnt, long maxBlockSize, Block block, SourceEntry<?> sourceEntry) {
        this.maxRowCnt = maxRowCnt;
        this.maxBlockSize = maxBlockSize;
        this.sourceEntry = sourceEntry;
        this.block = block;
    }

    public Block getBlock() {
        return block;
    }

    public boolean isEmpty() {
        return rowCnt == 0;
    }

    public boolean isFull() {
        return block.rowCnt() >= maxRowCnt || blockSize >= maxBlockSize;
    }

    public SourceEntry<?> getSourceEntry() {
        return sourceEntry;
    }

    public long getStartRowId() {
        return startRowId;
    }

    public long getEndRowId() {
        return endRowId;
    }

    public long getDataStartTime() {
        return dataStartTime;
    }

    public long getDataEndTime() {
        return dataEndTime;
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
