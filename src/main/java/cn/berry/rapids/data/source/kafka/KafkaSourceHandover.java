package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.exception.ClosedException;
import cn.berry.rapids.exception.WakeUpException;

public class KafkaSourceHandover implements CycleLife {

    private final Object lock = new Object();

    private SourceEntry<KafkaSourceEntry> next;

    private Throwable error;

    private boolean wakeupProducer;

    public SourceEntry<KafkaSourceEntry> pollNext() throws Throwable {
        synchronized (lock) {
            while (next == null && error == null) {
                lock.wait();
            }

            SourceEntry<KafkaSourceEntry> entry = next;
            if (entry != null) {
                next = null;
                lock.notifyAll();
                return entry;
            } else {
                rethrowException(error, error.getMessage());
                return null;
            }
        }
    }

    public void produce(final SourceEntry<KafkaSourceEntry> element) throws InterruptedException {
        synchronized (lock) {
            while (next != null && !wakeupProducer) {
                lock.wait();
            }
            wakeupProducer = true;
            if (next == null) {
                throw new WakeUpException();
            } else if (error == null) {
                next = element;
                lock.notifyAll();
            } else {
                throw new ClosedException();
            }
        }
    }

    public void wakeupProducer() {
        synchronized (lock) {
            wakeupProducer = true;
            lock.notifyAll();
        }
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public void stop() {
        synchronized (lock) {
            next = null;
            wakeupProducer = false;
            if (error == null)
                error = new ClosedException();
            lock.notifyAll();
        }
    }

    public void  reportError(Throwable t) {
        synchronized (lock) {
            if (error != null)
                error = t;
            next = null;
            lock.notifyAll();
        }
    }

    public static void rethrowException(Throwable t, String parentMessage) throws Throwable {
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        } else if (t instanceof Exception) {
            throw t;
        } else if (t instanceof Error) {
            throw (Error) t;
        } else {
            throw new RuntimeException(parentMessage + ": " + t.getMessage(), t);
        }

    }
}
