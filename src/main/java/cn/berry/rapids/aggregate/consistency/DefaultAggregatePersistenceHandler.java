package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.aggregate.consistency.impl.AbstractAggregatePersistenceHandler;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.BlockDataEvent;
import com.berry.clickhouse.tcp.client.ClickHouseClient;
import com.berry.clickhouse.tcp.client.data.Block;

/**
 * 默认持久化处理器
 * 
 * 描述: 处理区块事件的默认持久化逻辑，支持重试机制。
 * 
 * 特性:
 * 1. 支持重试机制
 * 2. 处理区块事件的存储
 * 
 * @author Berry
 * @version 1.0.0
 */
public class DefaultAggregatePersistenceHandler extends AbstractAggregatePersistenceHandler {

    private final ClickHouseClient clickHouseClient;

    private final int retryTimes;

    /**
     * 构造函数
     *
     * @param clickHouseClient ClickHouse客户端
     * @param configuration 应用配置对象
     */
    public DefaultAggregatePersistenceHandler(ClickHouseClient clickHouseClient, Configuration configuration) {
        this.clickHouseClient = clickHouseClient;
        int retryTimes = configuration.getSystemConfig().getAggregate().getInsertRetryTimes();
        if (retryTimes <= 0) {
            retryTimes = 3;
        }
        this.retryTimes = retryTimes;
    }

    @Override
    protected boolean flush() {
        return true;
    }

    /**
     * 处理区块事件
     *
     * @param event 区块事件
     * @return 是否处理成功
     */
    @Override
    protected boolean doHandle(BlockDataEvent event) {
        try {
            handleCanRetry(event.getMessage(), retryTimes, null);
        } catch (Throwable e) {
            // 处理异常
        }
        return false;
    }

    /**
     * 处理带重试的区块事件
     * 
     * @param block 区块
     * @param retryTimes 剩余重试次数
     * @param preError 之前的异常
     * @throws Throwable 处理过程中可能抛出的异常
     */
    private void handleCanRetry(Block block, int retryTimes, Throwable preError) throws Throwable {
        if (retryTimes <= 0 || isTerminal()) {
            throw preError;
        }
        try {
            this.clickHouseClient.insert(block);
        } catch (Throwable e) {
            handleCanRetry(block, retryTimes - 1, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start() throws Exception {
        // 启动逻辑
    }
}
