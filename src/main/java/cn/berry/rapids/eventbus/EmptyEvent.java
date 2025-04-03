package cn.berry.rapids.eventbus;

/**
 * 空事件类
 * 
 * 描述: 表示不包含任何消息的事件，用于特殊场景下的信号传递。
 * 
 * 特性:
 * 1. 继承自事件接口
 * 2. 不包含任何消息内容
 * 3. 提供单例实例
 * 
 * @author Berry
 * @version 1.0.0
 */
public class EmptyEvent implements Event<String> {

    /**
     * 空事件单例实例
     */
    public static final EmptyEvent INSTANCE = new EmptyEvent();

    /**
     * 获取事件类型
     * 
     * @return 事件类型字符串
     */
    @Override
    public String type() {
        return "empty";
    }

    /**
     * 获取事件消息
     * 
     * @return 由于是空事件，始终返回null
     */
    @Override
    public String getMessage() {
        return null;
    }
}
