package cn.berry.rapids.eventbus;

/**
 * 事件交接接口
 * 
 * 描述: 定义事件在生产者与消费者之间传递的基本行为。
 * 
 * 特性:
 * 1. 支持泛型事件类型
 * 2. 提供事件尝试投递功能
 * 3. 支持事件获取功能
 * 
 * @author Berry
 * @version 1.0.0
 */
public interface EventHandover {

    /**
     * 尝试投递事件
     * 
     * @param t 要投递的事件
     * @return 如果投递成功则返回true，否则返回false
     */
    boolean tryPost(Event<?> t);

    /**
     * 投递事件
     * 
     * @param t 要投递的事件
     * @param waitTime 等待时间
     * @return 如果投递成功则返回true，否则返回false
     * @throws InterruptedException 如果投递过程中被中断
     */
    boolean post(Event<?> t, long waitTime) throws InterruptedException;

    /**
     * 获取事件
     * 
     * @return 事件对象
     * @throws InterruptedException 如果获取过程中被中断
     */
    Event<?> get() throws InterruptedException;
}
