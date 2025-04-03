package cn.berry.rapids.eventbus;

/**
 * 事件接口
 * 
 * 描述: 定义事件的基本结构，作为所有事件类型的基接口。
 * 
 * 特性:
 * 1. 支持泛型消息类型
 * 2. 提供事件类型标识
 * 3. 支持消息内容管理
 * 
 * @author Berry
 * @version 1.0.0
 */
public interface Event<T> {

    /**
     * 获取事件类型
     * 
     * @return 事件类型的字符串表示
     */
    String type();

    /**
     * 获取事件消息
     * 
     * @return 事件消息对象
     */
    T getMessage();

    /**
     * 检查事件是否包含消息
     * 
     * @return 如果事件包含消息则返回true，否则返回false
     */
    default boolean hasMessage() {
        return null != getMessage();
    }

}
