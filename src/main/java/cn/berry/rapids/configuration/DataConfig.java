package cn.berry.rapids.configuration;

public class DataConfig {

    private Integer parseThreadSize;

    private Integer dataInsertQueue;

    private Integer dataInsertQueueNumber;

    private Integer dataInsertThreadSize;

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

    public Integer getDataInsertQueueNumber() {
        return dataInsertQueueNumber;
    }

    public void setDataInsertQueueNumber(Integer dataInsertQueueNumber) {
        this.dataInsertQueueNumber = dataInsertQueueNumber;
    }

    public Integer getDataInsertThreadSize() {
        return dataInsertThreadSize;
    }

    public void setDataInsertThreadSize(Integer dataInsertThreadSize) {
        this.dataInsertThreadSize = dataInsertThreadSize;
    }
}
