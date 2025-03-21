package cn.berry.rapids.eventbus;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class QueueEventHandover implements EventHandover {

    private static final int WARN_RATIO = 60;

    private final BlockingQueue<Event<?>> mainQueue;
    private final Map<String, SubQueue> subQueues;
    private final List<SubQueue> subQueueList;
    private final int queueSize;
    private int mainQueueRatio; // 主队列连续获取次数
    private int currentMainCount; // 当前主队列获取计数
    private int currentSubQueueIndex; // 当前副队列索引

    public QueueEventHandover(int queueSize, int mainQueueRatio) {
        this.mainQueue = new ArrayBlockingQueue<>(queueSize);
        this.subQueues = new HashMap<>();
        this.subQueueList = new ArrayList<>();
        this.queueSize = queueSize;
        this.mainQueueRatio = mainQueueRatio;
        this.currentMainCount = 0;
        this.currentSubQueueIndex = 0;
    }

    @Override
    public boolean tryPost(Event<?> t) {
        return mainQueue.offer(t);
    }

    @Override
    public boolean post(Event<?> t, long waitTime) throws InterruptedException {
        int ratio = (int) (((double) mainQueue.size() / (double) this.queueSize) * 100);
        if (ratio >= WARN_RATIO) {
            //print log
        }
        return mainQueue.offer(t, waitTime, TimeUnit.MILLISECONDS);
    }

    public SubQueue register(String key) {
        SubQueue subQueue = new SubQueue(new ArrayBlockingQueue<>(queueSize));
        subQueues.put(key, subQueue);
        subQueueList.add(subQueue);
        return subQueue;
    }

    @Override
    public Event<?> get() throws InterruptedException {
        Event<?> event = null;

        if (currentMainCount < mainQueueRatio) {
            // 从主队列获取
            event = mainQueue.poll();
            if (event != null) {
                currentMainCount++;
                return event;
            }
        }

        // 如果主队列为空或达到比例，尝试从副队列获取
        if (event == null && !subQueueList.isEmpty()) {
            while (event == null && currentSubQueueIndex < subQueueList.size()) {
                event = subQueueList.get(currentSubQueueIndex).queue.poll();
                currentSubQueueIndex = (currentSubQueueIndex + 1) % subQueueList.size();
            }

            // 如果完成一轮副队列循环，重置主队列计数
            if (currentSubQueueIndex == 0) {
                currentMainCount = 0;
            }
        }

        // 如果所有队列都为空，返回主队列的结果
        return event != null ? event : mainQueue.poll();
    }

    public void setMainQueueRatio(int mainQueueRatio) {
        this.mainQueueRatio = mainQueueRatio;
        this.currentMainCount = 0;
    }

    public static class SubQueue {
        private final BlockingQueue<Event<?>> queue;

        private SubQueue(BlockingQueue<Event<?>> queue) {
            this.queue = queue;
        }

        public boolean tryPost(Event<?> event) {
            return queue.offer(event);
        }

        public boolean post(Event<?> event, long waitTime) throws InterruptedException {
            return queue.offer(event, waitTime, TimeUnit.MILLISECONDS);
        }

        public BlockingQueue<Event<?>> getQueue() {
            return queue;
        }
    }
}