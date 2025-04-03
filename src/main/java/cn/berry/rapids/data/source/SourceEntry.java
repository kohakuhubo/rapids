package cn.berry.rapids.data.source;

/**
 * 数据源条目接口
 * 
 * 描述: 定义数据源条目的基本行为，提供数据的标识和处理状态管理。
 * 
 * 特性:
 * 1. 支持泛型数据类型
 * 2. 提供唯一标识
 * 3. 支持处理状态管理
 * 
 * @author Berry
 * @version 1.0.0
 */
public interface SourceEntry<T> {

    /**
     * 获取条目的唯一标识
     * 
     * @return 条目的唯一标识符
     */
    long id();

    /**
     * 获取条目的实际数据
     * 
     * @return 条目的实际数据对象
     */
    T entry();

    /**
     * 标记条目处理成功
     */
    void success();

    /**
     * 标记条目处理失败
     */
    void fail();

}
