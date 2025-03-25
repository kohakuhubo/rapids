package cn.berry.rapids.configuration;

public class BlockConfig {

    private Integer batchDataMaxRowCnt;

    private Integer batchDataMaxByteSize;

    private Integer blockSize;

    private Integer stringBlockSize;

    private Integer cacheLength;

    private Integer stackSize;

    private Integer selfBufferSize;

    private Integer stringStackSize;

    private Integer stringSelfBufferSize;

    public Integer getBatchDataMaxRowCnt() {
        return batchDataMaxRowCnt;
    }

    public void setBatchDataMaxRowCnt(Integer batchDataMaxRowCnt) {
        this.batchDataMaxRowCnt = batchDataMaxRowCnt;
    }

    public Integer getBatchDataMaxByteSize() {
        return batchDataMaxByteSize;
    }

    public void setBatchDataMaxByteSize(Integer batchDataMaxByteSize) {
        this.batchDataMaxByteSize = batchDataMaxByteSize;
    }

    public Integer getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(Integer blockSize) {
        this.blockSize = blockSize;
    }

    public Integer getStringBlockSize() {
        return stringBlockSize;
    }

    public void setStringBlockSize(Integer stringBlockSize) {
        this.stringBlockSize = stringBlockSize;
    }

    public Integer getCacheLength() {
        return cacheLength;
    }

    public void setCacheLength(Integer cacheLength) {
        this.cacheLength = cacheLength;
    }

    public Integer getStackSize() {
        return stackSize;
    }

    public void setStackSize(Integer stackSize) {
        this.stackSize = stackSize;
    }

    public Integer getSelfBufferSize() {
        return selfBufferSize;
    }

    public void setSelfBufferSize(Integer selfBufferSize) {
        this.selfBufferSize = selfBufferSize;
    }

    public Integer getStringStackSize() {
        return stringStackSize;
    }

    public void setStringStackSize(Integer stringStackSize) {
        this.stringStackSize = stringStackSize;
    }

    public Integer getStringSelfBufferSize() {
        return stringSelfBufferSize;
    }

    public void setStringSelfBufferSize(Integer stringSelfBufferSize) {
        this.stringSelfBufferSize = stringSelfBufferSize;
    }
}
