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
     * @param sourceType 数据源类型
     * @param sourceType 数据块
     * @param block 数据块对象
     */
    public SourceDataEvent(String sourceType, Block block, SourceEntry<?> sourceEntry) {
        this.sourceType = sourceType;
        this.block = block;
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
