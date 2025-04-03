package cn.berry.rapids.eventbus;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 队列事件交接类
 * 
 * 描述: 使用阻塞队列实现事件的传递，提供事件的生产者和消费者之间的同步机制。
 * 
 * 特性:
 * 1. 支持主队列和副队列
 * 2. 提供队列比例控制
 * 3. 支持事件超时投递
 * 
 * @author Berry
 * @version 1.0.0
 */
public class QueueEventHandover implements EventHandover {

    /**
     * 警告比例阈值
     */
    private static final int WARN_RATIO = 60;

    /**
     * 主队列
     */
    private final BlockingQueue<Event<?>> mainQueue;

    /**
     * 副队列映射
     */
    private final Map<String, SubQueue> subQueues;

    /**
     * 副队列列表
     */
    private final List<SubQueue> subQueueList;

    /**
     * 队列大小
     */
    private final int queueSize;

    /**
     * 主队列连续获取次数
     */
    private int mainQueueRatio;

    /**
     * 当前主队列获取计数
     */
    private int currentMainCount;

    /**
     * 当前副队列索引
     */
    private int currentSubQueueIndex;

    /**
     * 构造队列事件交接器
     * 
     * @param queueSize 队列大小
     * @param mainQueueRatio 主队列比例
     */
    public QueueEventHandover(int queueSize, int mainQueueRatio) {
        this.mainQueue = new ArrayBlockingQueue<>(queueSize);
        this.subQueues = new HashMap<>();
        this.subQueueList = new ArrayList<>();
        this.queueSize = queueSize;
        this.mainQueueRatio = mainQueueRatio;
        this.currentMainCount = 0;
        this.currentSubQueueIndex = 0;
    }

    /**
     * 尝试投递事件
     * 
     * @param t 要投递的事件
     * @return 如果投递成功则返回true，否则返回false
     */
    @Override
    public boolean tryPost(Event<?> t) {
        return mainQueue.offer(t);
    }

    /**
     * 投递事件
     * 
     * @param t 要投递的事件
     * @param waitTime 等待时间
     * @return 如果投递成功则返回true，否则返回false
     * @throws InterruptedException 如果投递过程中被中断
     */
    @Override
    public boolean post(Event<?> t, long waitTime) throws InterruptedException {
        int ratio = (int) (((double) mainQueue.size() / (double) this.queueSize) * 100);
        if (ratio >= WARN_RATIO) {
            //print log
        }
        return mainQueue.offer(t, waitTime, TimeUnit.MILLISECONDS);
    }

    /**
     * 注册副队列
     * 
     * @param key 副队列标识
     * @return 副队列对象
     */
    public SubQueue register(String key) {
        SubQueue subQueue = new SubQueue(new ArrayBlockingQueue<>(queueSize));
        subQueues.put(key, subQueue);
        subQueueList.add(subQueue);
        return subQueue;
    }

    /**
     * 获取事件
     * 
     * @return 事件对象
     * @throws InterruptedException 如果获取过程中被中断
     */
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

    /**
     * 设置主队列比例
     * 
     * @param mainQueueRatio 主队列比例
     */
    public void setMainQueueRatio(int mainQueueRatio) {
        this.mainQueueRatio = mainQueueRatio;
        this.currentMainCount = 0;
    }

    /**
     * 副队列内部类
     */
    public static class SubQueue {
        /**
         * 阻塞队列
         */
        private final BlockingQueue<Event<?>> queue;

        /**
         * 构造副队列
         * 
         * @param queue 阻塞队列
         */
        private SubQueue(BlockingQueue<Event<?>> queue) {
            this.queue = queue;
        }

        /**
         * 尝试投递事件
         * 
         * @param event 要投递的事件
         * @return 如果投递成功则返回true，否则返回false
         */
        public boolean tryPost(Event<?> event) {
            return queue.offer(event);
        }

        /**
         * 投递事件
         * 
         * @param event 要投递的事件
         * @param waitTime 等待时间
         * @return 如果投递成功则返回true，否则返回false
         * @throws InterruptedException 如果投递过程中被中断
         */
        public boolean post(Event<?> event, long waitTime) throws InterruptedException {
            return queue.offer(event, waitTime, TimeUnit.MILLISECONDS);
        }

        /**
         * 获取阻塞队列
         * 
         * @return 阻塞队列对象
         */
        public BlockingQueue<Event<?>> getQueue() {
            return queue;
        }
    }
}