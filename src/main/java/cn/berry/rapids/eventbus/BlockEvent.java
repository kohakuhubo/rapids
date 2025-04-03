package cn.berry.rapids.eventbus;

import com.berry.clickhouse.tcp.client.data.Block;

/**
 * 数据块事件类
 * 
 * 描述: 表示包含数据块的事件，用于在事件总线中传递数据块。
 * 
 * 特性:
 * 1. 继承自事件接口
 * 2. 支持数据块消息
 * 
 * @author Berry
 * @version 1.0.0
 */
public class BlockEvent implements Event<Block> {

    /**
     * 事件类型
     */
    private final String type;

    /**
     * 数据块对象
     */
    private final Block block;

    /**
     * 构造数据块事件
     * 
     * @param type 事件类型
     * @param block 数据块对象
     */
    public BlockEvent(String type, Block block) {
        this.type = type;
        this.block = block;
    }

    /**
     * 获取事件类型
     * 
     * @return 事件类型字符串
     */
    @Override
    public String type() {
        return this.type;
    }

    /**
     * 获取事件消息
     * 
     * @return 数据块对象
     */
    @Override
    public Block getMessage() {
        return this.block;
    }
}
