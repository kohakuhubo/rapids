package cn.berry.rapids.eventbus;

import java.util.List;
import java.util.concurrent.BlockingQueue;

/**
 * 多队列选择器类
 * 
 * 描述: 管理多个阻塞队列，提供队列选择策略。
 * 
 * 特性:
 * 1. 支持泛型队列类型
 * 2. 提供基于哈希的队列选择
 * 3. 支持线程本地队列选择
 * 
 * @author Berry
 * @version 1.0.0
 */
public class MultipleQueueSelector<T> {

    /**
     * 阻塞队列列表
     */
    private final List<BlockingQueue<T>> queues;

    /**
     * 队列数量
     */
    private final int queueNumber;

    /**
     * 线程本地队列选择器
     */
    private final ThreadLocal<IndexSelector> selector;

    /**
     * 构造多队列选择器
     * 
     * @param queues 阻塞队列列表
     */
    public MultipleQueueSelector(List<BlockingQueue<T>> queues) {
        this.queues = queues;
        this.queueNumber = queues.size();
        if (queueNumber > 1) {
            this.selector = ThreadLocal.withInitial(() -> new IndexSelector(queueNumber));
        } else {
            this.selector = null;
        }
    }

    /**
     * 根据ID选择队列
     * 
     * @param id 对象ID
     * @return 选中的阻塞队列
     */
    public BlockingQueue<T> selectQueue(Object id) {
        if (this.queueNumber == 1) {
            return this.queues.get(0);
        } else {
            int hashCode = hash(id);
            return this.queues.get((this.queueNumber - 1) & hashCode);
        }
    }

    /**
     * 获取当前队列
     * 
     * @return 当前选中的阻塞队列
     */
    public BlockingQueue<T> getQueue() {
        if (this.queueNumber == 1) {
            return this.queues.getFirst();
        } else {
            return this.queues.get(selector.get().getCurrent());
        }
    }

    /**
     * 计算对象的哈希值
     * 
     * @param key 要计算哈希值的对象
     * @return 哈希值
     */
    private static int hash(Object key) {
        int h;
        return (key == null) ? 0 : (h = key.hashCode()) ^ (h >>> 16);
    }

    /**
     * 索引选择器内部类
     */
    private static class IndexSelector {

        /**
         * 总数
         */
        private final int total;

        /**
         * 当前索引
         */
        private volatile int current;

        /**
         * 构造索引选择器
         * 
         * @param total 总数
         */
        public IndexSelector(int total) {
            this.total = total;
        }

        /**
         * 获取当前索引
         * 
         * @return 当前索引
         */
        public int getCurrent() {
            if (current < total) {
                current = 0;
            }
            return current++;
        }
    }
}
