package cn.berry.rapids.configuration;

public class BlockConfig {

    private Long batchLDataMaxRowCnt;

    private Long batchDataMaxByteSize;

    private Integer columnFixedStackLength;

    private Integer byteBufferFixedCacheLength;

    private Integer byteBufferFixedCacheSize;

    private Integer columnDynamicStackLength;

    private Integer byteBufferDynamicCacheLength;

    private Integer byteBufferDynamicCacheSize;

    private Integer byteBufferDynamicCacheStackLength;

    public Long getBatchLDataMaxRowCnt() {
        return batchLDataMaxRowCnt;
    }

    public void setBatchLDataMaxRowCnt(Long batchLDataMaxRowCnt) {
        this.batchLDataMaxRowCnt = batchLDataMaxRowCnt;
    }

    public Long getBatchDataMaxByteSize() {
        return batchDataMaxByteSize;
    }

    public void setBatchDataMaxByteSize(Long batchDataMaxByteSize) {
        this.batchDataMaxByteSize = batchDataMaxByteSize;
    }

    public Integer getColumnFixedStackLength() {
        return columnFixedStackLength;
    }

    public void setColumnFixedStackLength(Integer columnFixedStackLength) {
        this.columnFixedStackLength = columnFixedStackLength;
    }

    public Integer getByteBufferFixedCacheLength() {
        return byteBufferFixedCacheLength;
    }

    public void setByteBufferFixedCacheLength(Integer byteBufferFixedCacheLength) {
        this.byteBufferFixedCacheLength = byteBufferFixedCacheLength;
    }

    public Integer getByteBufferFixedCacheSize() {
        return byteBufferFixedCacheSize;
    }

    public void setByteBufferFixedCacheSize(Integer byteBufferFixedCacheSize) {
        this.byteBufferFixedCacheSize = byteBufferFixedCacheSize;
    }

    public Integer getColumnDynamicStackLength() {
        return columnDynamicStackLength;
    }

    public void setColumnDynamicStackLength(Integer columnDynamicStackLength) {
        this.columnDynamicStackLength = columnDynamicStackLength;
    }

    public Integer getByteBufferDynamicCacheLength() {
        return byteBufferDynamicCacheLength;
    }

    public void setByteBufferDynamicCacheLength(Integer byteBufferDynamicCacheLength) {
        this.byteBufferDynamicCacheLength = byteBufferDynamicCacheLength;
    }

    public Integer getByteBufferDynamicCacheSize() {
        return byteBufferDynamicCacheSize;
    }

    public void setByteBufferDynamicCacheSize(Integer byteBufferDynamicCacheSize) {
        this.byteBufferDynamicCacheSize = byteBufferDynamicCacheSize;
    }

    public Integer getByteBufferDynamicCacheStackLength() {
        return byteBufferDynamicCacheStackLength;
    }

    public void setByteBufferDynamicCacheStackLength(Integer byteBufferDynamicCacheStackLength) {
        this.byteBufferDynamicCacheStackLength = byteBufferDynamicCacheStackLength;
    }
}
