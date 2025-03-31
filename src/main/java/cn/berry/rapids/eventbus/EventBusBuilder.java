package cn.berry.rapids.eventbus;

import cn.berry.rapids.configuration.Configuration;

import java.util.LinkedList;
import java.util.List;

public class EventBusBuilder {

    private String eventType;

    private int queueSize;

    private String threadName;

    private List<Subscription<Event<?>>> subscriptions = new LinkedList<>();

    private EventHandover eventHandover;

    private long submitEventWaitTime = 1000L;

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

    public EventBusBuilder subscription(Subscription<Event<?>> val) {
        this.subscriptions.add(val);
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

    public EventBus build(Configuration configuration) {
        this.eventHandover = new QueueEventHandover(queueSize, 1);
        return new EventBus(this, configuration);
    }

}
