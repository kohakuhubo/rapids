package cn.berry.rapids.eventbus;

import cn.berry.rapids.configuration.Configuration;

import java.util.LinkedList;
import java.util.List;

/**
 * 事件总线构建器类
 * 
 * 描述: 提供事件总线的构建功能，支持配置事件总线的各种参数。
 * 
 * 特性:
 * 1. 支持泛型事件类型
 * 2. 提供事件总线参数配置
 * 3. 支持事件订阅配置
 * 
 * @author Berry
 * @version 1.0.0
 */
public class EventBusBuilder {

    /**
     * 队列大小
     */
    private int queueSize;

    /**
     * 线程名称
     */
    private String threadName;

    /**
     * 订阅者列表
     */
    private final List<Subscription<Event<?>>> subscriptions = new LinkedList<>();

    /**
     * 默认订阅者
     */
    private Subscription<Event<?>> defaultSubscriptions;

    /**
     * 事件交接器
     */
    private EventHandover eventHandover;

    /**
     * 提交事件等待时间
     */
    private long submitEventWaitTime = 1000L;

    /**
     * 线程大小
     */
    private int threadSize;

    /**
     * 设置提交事件等待时间
     * 
     * @param val 等待时间
     * @return 构建器本身
     */
    public EventBusBuilder submitEventWaitTime(long val) {
        this.submitEventWaitTime = val;
        return this;
    }

    /**
     * 设置队列大小
     * 
     * @param val 队列大小
     * @return 构建器本身
     */
    public EventBusBuilder queueSize(int val) {
        this.queueSize = val;
        return this;
    }

    /**
     * 设置线程名称
     * 
     * @param val 线程名称
     * @return 构建器本身
     */
    public EventBusBuilder threadName(String val) {
        this.threadName = val;
        return this;
    }

    /**
     * 设置线程大小
     * 
     * @param val 线程大小
     * @return 构建器本身
     */
    public EventBusBuilder threadSize(int val) {
        this.threadSize = val;
        return this;
    }

    /**
     * 添加订阅者
     * 
     * @param val 订阅者
     * @return 构建器本身
     */
    public EventBusBuilder subscription(Subscription<Event<?>> val) {
        this.subscriptions.add(val);
        return this;
    }

    /**
     * 设置默认订阅者
     * 
     * @param val 默认订阅者
     * @return 构建器本身
     */
    public EventBusBuilder defaultSubscription(Subscription<Event<?>> val) {
        this.defaultSubscriptions = val;
        return this;
    }

    /**
     * 添加订阅者列表
     * 
     * @param val 订阅者列表
     * @return 构建器本身
     */
    public EventBusBuilder subscription(List<Subscription<Event<?>>> val) {
        this.subscriptions.addAll(val);
        return this;
    }

    /**
     * 获取线程名称
     * 
     * @return 线程名称
     */
    public String getThreadName() {
        return threadName;
    }

    /**
     * 获取订阅者列表
     * 
     * @return 订阅者列表
     */
    public List<Subscription<Event<?>>> getSubscriptions() {
        return subscriptions;
    }

    /**
     * 获取事件交接器
     * 
     * @return 事件交接器
     */
    public EventHandover getEventHandover() {
        return eventHandover;
    }

    /**
     * 获取提交事件等待时间
     * 
     * @return 等待时间
     */
    public long getSubmitEventWaitTime() {
        return submitEventWaitTime;
    }

    /**
     * 获取线程大小
     * 
     * @return 线程大小
     */
    public int getThreadSize() {
        return threadSize;
    }

    /**
     * 获取默认订阅者
     * 
     * @return 默认订阅者
     */
    public Subscription<Event<?>> getDefaultSubscriptions() {
        return defaultSubscriptions;
    }

    /**
     * 构建事件总线
     * 
     * @param configuration 配置对象
     * @return 事件总线实例
     */
    public EventBus build(Configuration configuration) {
        this.eventHandover = new QueueEventHandover(queueSize, 1);
        return new EventBus(this, configuration);
    }
}
