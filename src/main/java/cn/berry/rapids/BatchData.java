package cn.berry.rapids;

/**
 * 批处理数据接口
 * 
 * 描述: 定义批量数据处理的基本属性和方法。
 * 此接口封装了数据批次的元信息，包括行ID范围、数据源信息、状态和时间范围等。
 * 实现此接口的类负责提供对批次数据的完整描述。
 * 
 * @author Berry
 * @version 1.0.0
 */
public interface BatchData {

    /**
     * 获取批次数据的起始行ID
     * 
     * @return 批次数据的起始行ID
     */
    long startRowId();

    /**
     * 获取批次数据的结束行ID
     * 
     * @return 批次数据的结束行ID
     */
    long endRowId();

    /**
     * 获取数据源主ID
     * 
     * @return 数据源的主标识符
     */
    String sourceMajorId();

    /**
     * 获取数据源次ID
     * 
     * @return 数据源的次标识符
     */
    String sourceMinorId();

    /**
     * 获取批次数据的状态
     * 
     * @return 表示批次数据当前状态的整数值
     */
    int status();

    /**
     * 获取批次数据的开始时间
     * 
     * @return 批次数据的开始时间戳（毫秒）
     */
    long dataStartTime();

    /**
     * 获取批次数据的结束时间
     * 
     * @return 批次数据的结束时间戳（毫秒）
     */
    long dataEndTime();

    /**
     * 获取聚合类型数组
     * 
     * @return 此批次数据适用的聚合类型数组
     */
    String[] aggregateTypes();

}
