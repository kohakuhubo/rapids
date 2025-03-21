package cn.berry.rapids.configuration;

public class AggregateConfig {

    private Integer aggregateWaitQueueSizeRatio;

    private Long aggregateWaitTime;

    private Integer aggregateThreadSize;

    private Integer aggregateWaitQueue;

    private Integer aggregateInsertQueue;

    private Integer insertBatchCount;

    private Long insertWaitTimeMillis;

    private Integer insertRetryTimes;

    public Integer getAggregateWaitQueueSizeRatio() {
        return aggregateWaitQueueSizeRatio;
    }

    public void setAggregateWaitQueueSizeRatio(Integer aggregateWaitQueueSizeRatio) {
        this.aggregateWaitQueueSizeRatio = aggregateWaitQueueSizeRatio;
    }

    public Long getAggregateWaitTime() {
        return aggregateWaitTime;
    }

    public void setAggregateWaitTime(Long aggregateWaitTime) {
        this.aggregateWaitTime = aggregateWaitTime;
    }

    public Integer getAggregateThreadSize() {
        return aggregateThreadSize;
    }

    public void setAggregateThreadSize(Integer aggregateThreadSize) {
        this.aggregateThreadSize = aggregateThreadSize;
    }

    public Integer getAggregateWaitQueue() {
        return aggregateWaitQueue;
    }

    public void setAggregateWaitQueue(Integer aggregateWaitQueue) {
        this.aggregateWaitQueue = aggregateWaitQueue;
    }

    public Integer getAggregateInsertQueue() {
        return aggregateInsertQueue;
    }

    public void setAggregateInsertQueue(Integer aggregateInsertQueue) {
        this.aggregateInsertQueue = aggregateInsertQueue;
    }

    public Integer getInsertBatchCount() {
        return insertBatchCount;
    }

    public void setInsertBatchCount(Integer insertBatchCount) {
        this.insertBatchCount = insertBatchCount;
    }

    public Long getInsertWaitTimeMillis() {
        return insertWaitTimeMillis;
    }

    public void setInsertWaitTimeMillis(Long insertWaitTimeMillis) {
        this.insertWaitTimeMillis = insertWaitTimeMillis;
    }

    public Integer getInsertRetryTimes() {
        return insertRetryTimes;
    }

    public void setInsertRetryTimes(Integer insertRetryTimes) {
        this.insertRetryTimes = insertRetryTimes;
    }
}
