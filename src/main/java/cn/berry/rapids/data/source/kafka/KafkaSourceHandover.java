package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.exception.ClosedException;
import cn.berry.rapids.exception.WakeUpException;

/**
 * Kafka数据源交接类
 * 
 * 描述: 负责将Kafka数据源的数据交接给数据解析服务。
 * 此类实现了生命周期接口，管理交接器的启动和停止。
 * 
 * 特性:
 * 1. 将数据交接给数据解析服务
 * 2. 管理数据交接的生命周期
 * 
 * @author Berry
 * @version 1.0.0
 */
public class KafkaSourceHandover implements CycleLife {

    private final Object lock = new Object();

    private SourceEntry<KafkaSourceEntry> next;

    private Throwable error;

    private boolean wakeupProducer;

    /**
     * 获取下一个数据源条目
     * 
     * @return 数据源条目
     * @throws Throwable 异常
     */
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

    /**
     * 生产数据源条目
     * 
     * @param element 数据源条目
     * @throws InterruptedException 中断异常
     */
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

    /**
     * 唤醒生产者
     */
    public void wakeupProducer() {
        synchronized (lock) {
            wakeupProducer = true;
            lock.notifyAll();
        }
    }

    /**
     * 启动Kafka数据源交接器
     * 
     * @throws Exception 启动过程中可能发生的任何异常
     */
    @Override
    public void start() throws Exception {

    }

    /**
     * 停止Kafka数据源交接器
     */
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

    /**
     * 报告错误
     * 
     * @param t 异常
     */
    public void  reportError(Throwable t) {
        synchronized (lock) {
            if (error != null)
                error = t;
            next = null;
            lock.notifyAll();
        }
    }

    /**
     * 重新抛出异常
     * 
     * @param t 异常
     * @param parentMessage 父消息
     * @throws Throwable 异常
     */
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
