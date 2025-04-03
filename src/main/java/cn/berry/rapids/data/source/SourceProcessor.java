package cn.berry.rapids.data.source;

/**
 * 数据源处理器接口
 * 
 * 描述: 定义数据源处理器的基本行为，负责处理解析后的数据。
 * 
 * 特性:
 * 1. 支持泛型数据类型
 * 2. 提供数据处理接口
 * 
 * @author Berry
 * @version 1.0.0
 */
public interface SourceProcessor<T> {

    /**
     * 处理数据源条目
     * 
     * @param entry 要处理的数据源条目
     */
    void process(SourceEntry<T> entry);

}
