package cn.berry.rapids.aggregate;

import cn.berry.rapids.BatchData;
import cn.berry.rapids.eventbus.Event;
import com.github.housepower.data.Block;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

public class DataWrapper implements Event<DataWrapper> {

    private final DataAggregateState state;

    private long dataStartTime;

    private long dataEndTime;

    private long startTimestamp;

    private List<Block> blocks;

    private BatchData batchData;

    public DataWrapper(BatchData batchData, int total) {
        this.dataStartTime = batchData.dataStartTime();
        this.dataEndTime = batchData.dataEndTime();
        this.batchData = batchData;

        String[] aggregateTypes;
        int aggregateTypesSize;
        if ((null == batchData.aggregateTypes() || batchData.aggregateTypes().length == 0)) {
            aggregateTypes = new String[]{"all"};
            aggregateTypesSize = total;
        } else {
            aggregateTypes = batchData.aggregateTypes();
            aggregateTypesSize = aggregateTypes.length;
        }
        this.state = new DataAggregateState(aggregateTypesSize, new HashSet<>(Arrays.asList(aggregateTypes)));
    }

    public DataAggregateState getState() {
        return state;
    }

    public long getDataStartTime() {
        return dataStartTime;
    }

    public long getDataEndTime() {
        return dataEndTime;
    }

    public long getStartTimestamp() {
        return startTimestamp;
    }

    public List<Block> getBlocks() {
        return blocks;
    }

    public BatchData getBatchData() {
        return batchData;
    }

    public void clean() {

    }

    @Override
    public String type() {
        return "base";
    }

    @Override
    public DataWrapper getMessage() {
        return this;
    }
}
