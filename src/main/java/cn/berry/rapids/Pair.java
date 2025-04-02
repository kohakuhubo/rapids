package cn.berry.rapids;

/**
 * 键值对类
 * 
 * 描述: 通用的键值对数据结构，用于存储两个相关联的值。
 * 此类是一个简单的泛型工具类，可以存储任意类型的键值对。
 * 
 * @param <K> 键的类型
 * @param <V> 值的类型
 * @author Berry
 * @version 1.0.0
 */
public class Pair<K, V> {
    
    /** 键 */
    private final K key;
    
    /** 值 */
    private final V value;
    
    /**
     * 构造一个键值对
     * 
     * @param key 键
     * @param value 值
     */
    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }
    
    /**
     * 获取键
     * 
     * @return 键
     */
    public K getKey() {
        return key;
    }
    
    /**
     * 获取值
     * 
     * @return 值
     */
    public V getValue() {
        return value;
    }
}
