package cn.berry.rapids.aggregate.state;

public class AggregateState {

    private final long startRowId;

    private final long endRowId;

    private final long dataStartTime;

    private final long dataEndTime;

    public AggregateState(Long startRowId, Long endRowId, long dataStartTime, long dataEndTime) {
        this.startRowId = startRowId;
        this.endRowId = endRowId;
        this.dataStartTime = dataStartTime;
        this.dataEndTime = dataEndTime;
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
}
