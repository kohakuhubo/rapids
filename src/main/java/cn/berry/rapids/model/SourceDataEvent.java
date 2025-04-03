package cn.berry.rapids.model;

import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.eventbus.Event;
import com.berry.clickhouse.tcp.client.data.Block;

/**
 * 基础数据类
 * 
 * 描述: 实现基础数据块的管理，用于处理数据块的大小限制和状态控制。
 * 
 * 特性:
 * 1. 实现Event接口，支持事件机制
 * 2. 支持数据块大小限制
 * 3. 支持数据源条目关联
 * 4. 提供数据块状态检查
 * 
 * @author Berry
 * @version 1.0.0
 */
public class SourceDataEvent implements Event<SourceDataEvent> {

    private String sourceType;

    /**
     * 最大行数限制
     */
    private final int maxRowCnt;

    /**
     * 最大数据块大小限制
     */
    private final long maxBlockSize;

    /**
     * 数据源条目
     */
    private SourceEntry<?> sourceEntry;

    /**
     * 数据块对象
     */
    private final Block block;

    /**
     * 构造基础数据对象
     * 
     * @param maxRowCnt 最大行数限制
     * @param maxBlockSize 最大数据块大小限制
     * @param block 数据块对象
     */
    public SourceDataEvent(String sourceType, int maxRowCnt, long maxBlockSize, Block block) {
        this.sourceType = sourceType;
        this.maxRowCnt = maxRowCnt;
        this.maxBlockSize = maxBlockSize;
        this.block = block;
    }

    /**
     * 设置数据源条目
     * 
     * @param sourceEntry 数据源条目
     */
    public void setSourceEntry(SourceEntry<?> sourceEntry) {
        this.sourceEntry = sourceEntry;
    }

    /**
     * 获取数据块对象
     * 
     * @return 数据块对象
     */
    public Block getBlock() {
        return block;
    }

    /**
     * 检查数据块是否为空
     * 
     * @return 如果数据块行数为0则返回true，否则返回false
     */
    public boolean isEmpty() {
        return block.rowCnt() == 0;
    }

    /**
     * 检查数据块是否已满
     * 
     * @return 如果数据块行数达到最大限制则返回true，否则返回false
     */
    public boolean isFull() {
        return block.rowCnt() >= maxRowCnt;
    }

    /**
     * 获取数据源条目
     * 
     * @return 数据源条目
     */
    public SourceEntry<?> getSourceEntry() {
        return sourceEntry;
    }

    /**
     * 获取事件类型
     * 
     * @return 事件类型字符串
     */
    @Override
    public String type() {
        return this.sourceType;
    }

    /**
     * 获取事件消息
     * 
     * @return 当前对象作为事件消息
     */
    @Override
    public SourceDataEvent getMessage() {
        return this;
    }
}
