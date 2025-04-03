package cn.berry.rapids.util;

/**
 * ID和时间戳对类
 * 
 * 描述: 封装ID和时间戳的关联关系，用于记录ID生成时的时间信息。
 * 
 * 特性:
 * 1. 支持ID和时间戳的关联
 * 2. 提供不可变的数据结构
 * 3. 支持记录创建时间
 * 
 * @author Berry
 * @version 1.0.0
 */
public class IdTimestampPair {

    /**
     * ID值
     */
    private final long id;

    /**
     * 时间戳
     */
    private final long timestamp;

    /**
     * 构造ID和时间戳对
     * 
     * @param id ID值
     * @param timestamp 时间戳
     */
    public IdTimestampPair(long id, long timestamp) {
        this.id = id;
        this.timestamp = timestamp;
    }

    /**
     * 获取ID值
     * 
     * @return ID值
     */
    public long getId() {
        return id;
    }

    /**
     * 获取时间戳
     * 
     * @return 时间戳
     */
    public long getTimestamp() {
        return timestamp;
    }
}
