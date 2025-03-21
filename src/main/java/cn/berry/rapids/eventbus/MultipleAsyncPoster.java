package cn.berry.rapids.eventbus;

import cn.berry.rapids.Stoppable;

public class MultipleAsyncPoster extends Stoppable implements Runnable {

    private final MultipleQueueSelector<Event<?>> receiver;

    private final Subscription<Event<?>> subscription;

    private final  long waitTimeMillis;

    private long currentWaitTimeMillis = 0L;

    private long preWaitTimestamp = 0L;

    private volatile boolean isTerminal = true;

    public MultipleAsyncPoster(MultipleQueueSelector<Event<?>> receiver, Subscription<Event<?>> subscription, long waitTimeMillis) {
        this.receiver = receiver;
        this.subscription = subscription;
        this.waitTimeMillis = waitTimeMillis;
    }

    @Override
    public void run() {
        while (isTerminal && !Thread.currentThread().isInterrupted()) {
            Event<?> event = null;
            try {
                event = receiver.getQueue().poll();
            } catch (Throwable e) {
                //ignore
            }
            if (Thread.currentThread().isInterrupted() || stopped) {
                break;
            }
            boolean force = loadWaitTimeMillis() >= waitTimeMillis;
            if (force) {
                clearWaitTimeMillis();
            }
            if (event != null || force) {
                try {
                    if (event == null) {
                        subscription.onMessage(EmptyEvent.INSTANCE);
                    } else {
                        subscription.onMessage(event);
                    }
                } catch (Throwable e) {
                    //ignore
                }
            } else {
                try {
                    Thread.sleep(200L);
                } catch (Exception e) {
                    //ignore
                }
            }
        }
        this.isTerminal = true;
    }

    @Override
    public void stop() {
        super.stop();
    }

    public Subscription<?> getSubscription() {
        return subscription;
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
