package cn.berry.rapids.aggregate;

import cn.berry.rapids.eventbus.BlockDataEvent;

/**
 * 聚合服务处理器接口
 * 
 * 描述: 定义聚合服务处理的基本方法。
 * 实现此接口的类负责处理数据包装。
 * 
 * @author Berry
 * @version 1.0.0
 */
public interface AggregateServiceHandler {

    /**
     * 处理数据包装
     * 
     * @param blockDataEvent 数据对象
     */
    void handle(BlockDataEvent blockDataEvent);
}