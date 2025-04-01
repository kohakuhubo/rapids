package cn.berry.rapids.eventbus;

import cn.berry.rapids.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class EventReceiver extends Stoppable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(EventReceiver.class);

    private final EventHandover eventHandover;

    private final Subscription<Event<?>> defaultSubscription;

    private final Map<String, List<Subscription<Event<?>>>> subscriptionMap;

    private final  long waitTimeMillis;

    private long currentWaitTimeMillis = 0L;

    private long preWaitTimestamp = 0L;

    private volatile boolean isTerminal = true;

    public EventReceiver(EventHandover eventHandover, Subscription<Event<?>> defaultSubscription,
                         Map<String, List<Subscription<Event<?>>>> subscriptionMap, long waitTimeMillis) {
        this.eventHandover = eventHandover;
        this.defaultSubscription = defaultSubscription;
        this.subscriptionMap = subscriptionMap;
        this.waitTimeMillis = waitTimeMillis;
    }

    @Override
    public void run() {
        while (!isTerminal && !Thread.currentThread().isInterrupted()) {
            Event<?> event = null;
            try {
                event = this.eventHandover.get();
            } catch (Throwable e) {
                logger.error("event handover error", e);
            }
            if (Thread.currentThread().isInterrupted() || stopped) {
                break;
            }
            boolean force = loadWaitTimeMillis() >= waitTimeMillis;
            if (force) {
                clearWaitTimeMillis();
            }
            if (event != null || force) {
                event = (null == event) ? EmptyEvent.INSTANCE : event;
                List<Subscription<Event<?>>> subscriptions = subscriptionMap.get(event.type());
                if (null != subscriptions && !subscriptions.isEmpty()) {
                    for (Subscription<Event<?>> subscription : subscriptions) {
                        try {
                            subscription.onMessage(event);
                        } catch (Throwable e) {
                            logger.error("subscription[{}] error", subscription.id(), e);
                        }
                    }
                } else if (null != defaultSubscription) {
                    try {
                        defaultSubscription.onMessage(event);
                    } catch (Throwable e) {
                        logger.error("default subscription[{}] error", defaultSubscription.id(), e);
                    }
                } else {
                    logger.warn("no subscription for event type: {}", event.type());
                }
            } else {
                try {
                    Thread.sleep(200L);
                } catch (Exception e) {
                    logger.error("sleep error", e);
                }
            }
        }
        this.isTerminal = true;
    }

    @Override
    public void stop() {
        super.stop();
    }

    private long loadWaitTimeMillis() {
        long timestamp = System.currentTimeMillis();
        if (this.preWaitTimestamp > 0L) {
            this.currentWaitTimeMillis += (timestamp - this.preWaitTimestamp);
        }
        this.preWaitTimestamp = timestamp;
        return this.currentWaitTimeMillis;
    }

    private void clearWaitTimeMillis() {
        currentWaitTimeMillis = 0L;
        preWaitTimestamp = 0L;
    }

    public boolean isTerminal() {
        return isTerminal;
    }
}
