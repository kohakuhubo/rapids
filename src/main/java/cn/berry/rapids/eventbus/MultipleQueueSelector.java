package cn.berry.rapids.eventbus;

import java.util.List;
import java.util.concurrent.BlockingQueue;

public class MultipleQueueSelector<T> {

    private final List<BlockingQueue<T>> queues;

    private final int queueNumber;

    private final ThreadLocal<IndexSelector> selector;

    public MultipleQueueSelector(List<BlockingQueue<T>> queues) {
        this.queues = queues;
        this.queueNumber = queues.size();
        if (queueNumber > 1) {
            this.selector = ThreadLocal.withInitial(() -> new IndexSelector(queueNumber));
        } else {
            this.selector = null;
        }
    }

    public BlockingQueue<T> selectQueue(Object id) {
        if (this.queueNumber == 1) {
            return this.queues.get(0);
        } else {
            int hashCode = hash(id);
            return this.queues.get((this.queueNumber - 1) & hashCode);
        }
    }

    public BlockingQueue<T> getQueue() {
        if (this.queueNumber == 1) {
            return this.queues.getFirst();
        } else {
            return this.queues.get(selector.get().getCurrent());
        }
    }

    private static int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }


    private static class IndexSelector {

        private final int total;

        private volatile int current;

        public IndexSelector(int total) {
            this.total = total;
        }

        public int getCurrent() {
            if (current < total) {
                current = 0;
            }
            return current++;
        }
    }


}
