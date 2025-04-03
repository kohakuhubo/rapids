package cn.berry.rapids.eventbus;

import cn.berry.rapids.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 多异步投递器类
 * 
 * 描述: 提供异步事件投递功能，支持多个事件同时投递。
 * 
 * 特性:
 * 1. 支持泛型事件类型
 * 2. 使用线程池实现异步投递
 * 3. 提供事件投递超时控制
 * 
 * @author Berry
 * @version 1.0.0
 */
public class MultipleAsyncPoster extends Stoppable implements Runnable {

    /**
     * 日志记录器
     */
    private static final Logger logger = LoggerFactory.getLogger(MultipleAsyncPoster.class);

    /**
     * 多队列选择器
     */
    private final MultipleQueueSelector<Event<?>> receiver;

    /**
     * 订阅者
     */
    private final Subscription<Event<?>> subscription;

    /**
     * 等待时间（毫秒）
     */
    private final long waitTimeMillis;

    /**
     * 当前等待时间（毫秒）
     */
    private long currentWaitTimeMillis = 0L;

    /**
     * 上次等待时间戳
     */
    private long preWaitTimestamp = 0L;

    /**
     * 是否终止标志
     */
    private volatile boolean isTerminal = true;

    /**
     * 构造多异步投递器
     * 
     * @param receiver 多队列选择器
     * @param subscription 订阅者
     * @param waitTimeMillis 等待时间（毫秒）
     */
    public MultipleAsyncPoster(MultipleQueueSelector<Event<?>> receiver, Subscription<Event<?>> subscription, long waitTimeMillis) {
        this.receiver = receiver;
        this.subscription = subscription;
        this.waitTimeMillis = waitTimeMillis;
    }

    /**
     * 运行异步投递器
     */
    @Override
    public void run() {
        while (isTerminal && !Thread.currentThread().isInterrupted()) {
            Event<?> event = null;
            try {
                event = receiver.getQueue().poll();
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
                try {
                    if (event == null) {
                        subscription.onMessage(EmptyEvent.INSTANCE);
                    } else {
                        subscription.onMessage(event);
                    }
                } catch (Throwable e) {
                    logger.error("subscription[" + subscription.type() + "] error", e);
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

    /**
     * 停止异步投递器
     */
    @Override
    public void stop() {
        super.stop();
    }

    /**
     * 获取订阅者
     * 
     * @return 订阅者对象
     */
    public Subscription<?> getSubscription() {
        return subscription;
    }

    /**
     * 加载等待时间
     * 
     * @return 当前等待时间（毫秒）
     */
    private long loadWaitTimeMillis() {
        long timestamp = System.currentTimeMillis();
        if (this.preWaitTimestamp > 0L) {
            this.currentWaitTimeMillis += (timestamp - this.preWaitTimestamp);
        }
        this.preWaitTimestamp = timestamp;
        return this.currentWaitTimeMillis;
    }

    /**
     * 清除等待时间
     */
    private void clearWaitTimeMillis() {
        currentWaitTimeMillis = 0L;
        preWaitTimestamp = 0L;
    }

    /**
     * 检查是否终止
     * 
     * @return 如果已终止则返回true，否则返回false
     */
    public boolean isTerminal() {
        return isTerminal;
    }
}
