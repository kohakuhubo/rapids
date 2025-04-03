package cn.berry.rapids.configuration;

public class DataConfig {

    private Integer parseThreadSize;

    private Integer dataInsertQueue;

    private Integer dataInsertWaitTimeMills;

    private Integer dataInsertThreadSize;

    private Integer dataInsertRetryTimes;

    public Integer getParseThreadSize() {
        return parseThreadSize;
    }

    public void setParseThreadSize(Integer parseThreadSize) {
        this.parseThreadSize = parseThreadSize;
    }

    public Integer getDataInsertQueue() {
        return dataInsertQueue;
    }

    public void setDataInsertQueue(Integer dataInsertQueue) {
        this.dataInsertQueue = dataInsertQueue;
    }

    public void setDataInsertWaitTimeMills(Integer dataInsertWaitTimeMills) {
        this.dataInsertWaitTimeMills = dataInsertWaitTimeMills;
    }

    public Integer getDataInsertWaitTimeMills() {
        return dataInsertWaitTimeMills;
    }

    public Integer getDataInsertThreadSize() {
        return dataInsertThreadSize;
    }

    public void setDataInsertThreadSize(Integer dataInsertThreadSize) {
        this.dataInsertThreadSize = dataInsertThreadSize;
    }

    public Integer getDataInsertRetryTimes() {
        return dataInsertRetryTimes;
    }

    public void setDataInsertRetryTimes(Integer dataInsertRetryTimes) {
        this.dataInsertRetryTimes = dataInsertRetryTimes;
    }
}
