package cn.berry.rapids.eventbus;

import cn.berry.rapids.configuration.Configuration;

import java.util.LinkedList;
import java.util.List;

public class EventBusBuilder {

    private String eventType;

    private int queueSize;

    private String threadName;

    private final List<Subscription<Event<?>>> subscriptions = new LinkedList<>();

    private Subscription<Event<?>> defaultSubscriptions;

    private EventHandover eventHandover;

    private long submitEventWaitTime = 1000L;

    private int threadSize;

    public EventBusBuilder eventType(String val) {
        this.eventType = val;
        return this;
    }

    public EventBusBuilder submitEventWaitTime(long val) {
        this.submitEventWaitTime = val;
        return this;
    }

    public EventBusBuilder queueSize(int val) {
        this.queueSize = val;
        return this;
    }

    public EventBusBuilder threadName(String val) {
        this.threadName = val;
        return this;
    }

    public EventBusBuilder threadSize(int val) {
        this.threadSize = val;
        return this;
    }

    public EventBusBuilder subscription(Subscription<Event<?>> val) {
        this.subscriptions.add(val);
        return this;
    }

    public EventBusBuilder defaultSubscription(Subscription<Event<?>> val) {
        this.defaultSubscriptions = val;
        return this;
    }

    public EventBusBuilder subscription(List<Subscription<Event<?>>> val) {
        this.subscriptions.addAll(val);
        return this;
    }

    public String getEventType() {
        return eventType;
    }

    public String getThreadName() {
        return threadName;
    }

    public List<Subscription<Event<?>>> getSubscriptions() {
        return subscriptions;
    }

    public EventHandover getEventHandover() {
        return eventHandover;
    }

    public long getSubmitEventWaitTime() {
        return submitEventWaitTime;
    }

    public int getThreadSize() {
        return threadSize;
    }

    public Subscription<Event<?>> getDefaultSubscriptions() {
        return defaultSubscriptions;
    }

    public EventBus build(Configuration configuration) {
        this.eventHandover = new QueueEventHandover(queueSize, 1);
        return new EventBus(this, configuration);
    }

}
