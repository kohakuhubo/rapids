package cn.berry.rapids.model;

import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.eventbus.Event;

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

    public BaseData(int maxRowCnt, long maxBlockSize, SourceEntry<?> sourceEntry) {
        this.maxRowCnt = maxRowCnt;
        this.maxBlockSize = maxBlockSize;
        this.sourceEntry = sourceEntry;
    }

    public void increment() {
        rowCnt++;
    }

    public boolean isEmpty() {
        return rowCnt == 0;
    }

    public boolean isFull() {
        return rowCnt >= maxRowCnt || blockSize >= maxBlockSize;
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
