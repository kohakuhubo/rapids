package cn.berry.rapids.eventbus;

import cn.berry.rapids.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * 事件总线类
 * 
 * 描述: 实现事件的生产者和消费者之间的通信，管理事件的投递和处理。
 * 
 * 特性:
 * 1. 支持泛型事件类型
 * 2. 提供事件投递和处理功能
 * 3. 支持异步事件处理
 * 
 * @author Berry
 * @version 1.0.0
 */
public class EventBus {

    /**
     * 日志记录器
     */
    private static final Logger logger = LoggerFactory.getLogger(EventBus.class);

    /**
     * 线程池服务
     */
    private final ExecutorService executorService;

    /**
     * 事件交接器
     */
    private final EventHandover eventHandover;

    /**
     * 事件接收器列表
     */
    private final List<EventReceiver> eventReceivers;

    /**
     * 运行中的异步投递器列表
     */
    private final List<RunningAsyncPoster> runningAsyncPosters;

    /**
     * 等待时间（毫秒）
     */
    private final long waitTimeMills;

    /**
     * 是否停止标志
     */
    private volatile boolean isStopped = false;

    /**
     * 订阅者映射
     */
    private final Map<String, List<Subscription<Event<?>>>> subscriptionMap;

    /**
     * 默认订阅者
     */
    private final Subscription<Event<?>> defaultSubscription;

    /**
     * 创建事件总线构建器
     * 
     * @return 事件总线构建器
     */
    public static EventBusBuilder newEventBusBuilder() {
        return new EventBusBuilder();
    }

    /**
     * 构造事件总线
     * 
     * @param eventBusBuilder 事件总线构建器
     * @param configuration 配置对象
     */
    public EventBus(EventBusBuilder eventBusBuilder, Configuration configuration) {
        this.waitTimeMills = eventBusBuilder.getSubmitEventWaitTime();

        this.eventReceivers = new CopyOnWriteArrayList<>();
        this.eventHandover = eventBusBuilder.getEventHandover();

        this.defaultSubscription = eventBusBuilder.getDefaultSubscriptions();
        List<Subscription<Event<?>>> subscriptions = eventBusBuilder.getSubscriptions();
        if (eventBusBuilder.getDefaultSubscriptions() != null) {
            this.subscriptionMap = subscriptions.stream().collect(Collectors.groupingBy(Subscription::type));
        } else {
            this.subscriptionMap = Collections.emptyMap();
        }

        int threadSize = eventBusBuilder.getThreadSize();
        for (int i = 0; i < threadSize; i++) {
            this.eventReceivers.add(new EventReceiver(this.eventHandover, this.defaultSubscription, this.subscriptionMap,
                    configuration.getSystemConfig().getAggregate().getInsertWaitTimeMillis()));
        }

        if (threadSize <= 1) {
            this.executorService = Executors.newSingleThreadExecutor();
        } else {
            this.executorService = new ThreadPoolExecutor(threadSize, threadSize, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>());
        }
        this.runningAsyncPosters = new ArrayList<>(eventReceivers.size());
        for (EventReceiver receiver : eventReceivers) {
            Future<?> future = executorService.submit(receiver);
            runningAsyncPosters.add(new RunningAsyncPoster(receiver, future));
        }
    }

    /**
     * 停止事件总线
     */
    public void stop() {
        this.isStopped = true;
        runningAsyncPosters.forEach(r -> r.asyncPoster().stop());
        runningAsyncPosters.forEach(r -> {
            EventReceiver receiver = r.asyncPoster();
            while (!receiver.isTerminal()) {
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    logger.error("stop error", e);
                }
            }
        });
        this.executorService.shutdown();
    }

    /**
     * 尝试投递事件
     * 
     * @param event 要投递的事件
     * @return 如果投递成功则返回true，否则返回false
     */
    public boolean tryPost(Event<?> event) {
        if (isStopped) {
            return false;
        }
        return eventHandover.tryPost(event);
    }

    /**
     * 异步投递事件
     * 
     * @param event 要投递的事件
     */
    public void postAsync(Event<?> event) {
        postAsync(event, this.waitTimeMills);
    }

    /**
     * 异步投递事件
     * 
     * @param event 要投递的事件
     * @param waitTime 等待时间
     */
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
                logger.error("postAsync error", e);
            }
        } while (!isStopped);
    }

    /**
     * 运行中的异步投递器记录类
     */
    private record RunningAsyncPoster(EventReceiver asyncPoster, Future<?> future) {
    }
}
