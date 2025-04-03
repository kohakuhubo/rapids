package cn.berry.rapids.eventbus;

import cn.berry.rapids.Stoppable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * 事件接收器类
 * 
 * 描述: 负责接收和处理事件，将事件分发给相应的订阅者。
 * 
 * 特性:
 * 1. 支持泛型事件类型
 * 2. 提供事件接收和处理功能
 * 3. 支持多个订阅者
 * 
 * @author Berry
 * @version 1.0.0
 */
public class EventReceiver extends Stoppable implements Runnable {

    /**
     * 日志记录器
     */
    private static final Logger logger = LoggerFactory.getLogger(EventReceiver.class);

    /**
     * 事件交接器
     */
    private final EventHandover eventHandover;

    /**
     * 默认订阅者
     */
    private final Subscription<Event<?>> defaultSubscription;

    /**
     * 订阅者映射
     */
    private final Map<String, List<Subscription<Event<?>>>> subscriptionMap;

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
     * 构造事件接收器
     * 
     * @param eventHandover 事件交接器
     * @param defaultSubscription 默认订阅者
     * @param subscriptionMap 订阅者映射
     * @param waitTimeMillis 等待时间（毫秒）
     */
    public EventReceiver(EventHandover eventHandover, Subscription<Event<?>> defaultSubscription,
                         Map<String, List<Subscription<Event<?>>>> subscriptionMap, long waitTimeMillis) {
        this.eventHandover = eventHandover;
        this.defaultSubscription = defaultSubscription;
        this.subscriptionMap = subscriptionMap;
        this.waitTimeMillis = waitTimeMillis;
    }

    /**
     * 运行事件接收器
     */
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
                            logger.error("subscription[{}] error", subscription.type(), e);
                        }
                    }
                } else if (null != defaultSubscription) {
                    try {
                        defaultSubscription.onMessage(event);
                    } catch (Throwable e) {
                        logger.error("default subscription[{}] error", defaultSubscription.type(), e);
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

    /**
     * 停止事件接收器
     */
    @Override
    public void stop() {
        super.stop();
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
