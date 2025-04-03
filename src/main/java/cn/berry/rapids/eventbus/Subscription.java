package cn.berry.rapids.eventbus;

/**
 * 订阅接口
 * 
 * 描述: 定义事件订阅者的基本行为，用于处理特定类型的事件。
 * 
 * 特性:
 * 1. 支持泛型消息类型
 * 2. 提供订阅者标识
 * 3. 支持事件处理回调
 * 
 * @author Berry
 * @version 1.0.0
 */
public interface Subscription<T> {

    /**
     * 获取订阅的事件类型
     * 
     * @return 事件类型的字符串表示
     */
    String type();

    /**
     * 处理事件消息
     * 
     * @param event 事件消息对象
     */
    void onMessage(T event);

}
