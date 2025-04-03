package cn.berry.rapids.data.source;

/**
 * 数据源接口
 * 
 * 描述: 定义数据源的基本行为，提供数据的读取和提交功能。
 * 
 * 特性:
 * 1. 支持泛型数据类型
 * 2. 提供数据连续性检查
 * 3. 支持数据提交功能
 * 
 * @author Berry
 * @version 1.0.0
 */
public interface Source<T> {

    /**
     * 检查数据源是否可以继续提供数据
     * 
     * @return 如果数据源可以继续提供数据则返回true，否则返回false
     */
    boolean continuable();

    /**
     * 获取下一个数据条目
     * 
     * @return 数据源条目对象，包含实际数据
     */
    SourceEntry<T> next();

    /**
     * 提交已处理的数据条目
     * 
     * @param entry 要提交的数据条目
     */
    void commit(SourceEntry<T> entry);

}
