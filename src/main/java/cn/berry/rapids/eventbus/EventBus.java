package cn.berry.rapids.eventbus;

import cn.berry.rapids.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class EventBus {

    private final String eventType;

    private final ExecutorService executorService;

    private final EventHandover eventHandover;

    private final List<EventReceiver> eventReceivers;

    private final List<RunningAsyncPoster> runningAsyncPosters;

    private final long waitTimeMills;

    private volatile boolean isStopped = false;

    public static EventBusBuilder newEventBusBuilder() {
        return new EventBusBuilder();
    }

    public EventBus(EventBusBuilder eventBusBuilder, Configuration configuration) {
        this.eventType = eventBusBuilder.getEventType();
        this.waitTimeMills = eventBusBuilder.getSubmitEventWaitTime();

        Map<String, Subscription<?>> subscriptionMap = new ConcurrentHashMap<>();
        this.eventReceivers = new CopyOnWriteArrayList<>();
        this.eventHandover = eventBusBuilder.getEventHandover();

        for (Subscription<Event<?>> subscription : eventBusBuilder.getSubscriptions()) {
            if (!subscriptionMap.containsKey(subscription.id())) {
                subscriptionMap.put(subscription.id(), subscription);
                this.eventReceivers.add(new EventReceiver(this.eventHandover, subscription,
                        configuration.getSystemConfig().getAggregate().getInsertWaitTimeMillis()));
            }
        }

        if (eventReceivers.size() <= 1) {
            this.executorService = Executors.newSingleThreadExecutor();
        } else {
            this.executorService = new ThreadPoolExecutor(eventReceivers.size(), eventReceivers.size(), 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());
        }
        this.runningAsyncPosters = new ArrayList<>(eventReceivers.size());
        for (EventReceiver receiver : eventReceivers) {
            Future<?> future = executorService.submit(receiver);
            runningAsyncPosters.add(new RunningAsyncPoster(receiver, future));
        }
    }

    public void stop() {
        this.isStopped = true;
        runningAsyncPosters.forEach(r -> r.asyncPoster().stop());
        runningAsyncPosters.forEach(r -> {
            EventReceiver receiver = r.asyncPoster();
            while (!receiver.isTerminal()) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    //ignore
                }
            }
        });
        this.executorService.shutdown();
    }

    public boolean tryPost(Event<?> event) {
        if (isStopped) {
            return false;
        }
        return eventHandover.tryPost(event);
    }

    public void postAsync(Event<?> event) {
        postAsync(event, this.waitTimeMills);
    }

    public void postAsync(Event<?> event, long waitTime) {
        if (isStopped) {
            return;
        }
        long time = (waitTime > 0) ? waitTime : this.waitTimeMills;
        do {
            try {
                if (eventHandover.post(event, time))
                    break;
            } catch (InterruptedException e) {
                //ignore
            }
        } while (!isStopped);
    }

    public String getEventType() {
        return eventType;
    }

    private record RunningAsyncPoster(EventReceiver asyncPoster, Future<?> future) {
    }

}
