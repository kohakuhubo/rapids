package cn.berry.rapids.data.persistece;

import cn.berry.rapids.aggregate.AggregateServiceHandler;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.BlockDataEvent;
import cn.berry.rapids.model.SourceDataEvent;
import com.berry.clickhouse.tcp.client.ClickHouseClient;
import com.berry.clickhouse.tcp.client.data.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultSourceDataPersistenceHandler extends AbstractSourceDataPersistenceHandler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultSourceDataPersistenceHandler.class);

    private final int retryTimes;

    @Override
    protected void flush() {

    }

    public DefaultSourceDataPersistenceHandler(Configuration configuration, AggregateServiceHandler aggregateServiceHandler, ClickHouseClient clickHouseClient) {
        setConfiguration(configuration);
        setAggregateServiceHandler(aggregateServiceHandler);
        setClickHouseClient(clickHouseClient);
        int retryTimes = configuration.getSystemConfig().getData().getDataInsertRetryTimes();
        if (retryTimes <= 0) {
            retryTimes = 3;
        }
        this.retryTimes = retryTimes;
    }

    /**
     * 处理区块事件
     *
     * @param sourceDataEvent 基础数据
     * @return 是否处理成功
     */
    @Override
    public void doHandle(SourceDataEvent sourceDataEvent) {
        boolean res = false;
        try {
            res = handleCanRetry(sourceDataEvent.getBlock(), retryTimes, null);
        } catch (Throwable e) {
            logger.error("handle block has error! retry times:" + retryTimes, e);
        }
        //如果处理成功，提交到聚合服务处理器
        if (res) {
            AggregateServiceHandler aggregateServiceHandler = getAggregateServiceHandler();
            if (aggregateServiceHandler != null) {
                aggregateServiceHandler.handle(new BlockDataEvent(sourceDataEvent.type(), sourceDataEvent.getBlock()));
            }
        }
    }

    /**
     * 处理带重试的区块事件
     *
     * @param block 区块
     * @param retryTimes 剩余重试次数
     * @param preError 之前的异常
     * @throws Throwable 处理过程中可能抛出的异常
     */
    private boolean handleCanRetry(Block block, int retryTimes, Throwable preError) throws Throwable {
        if (retryTimes <= 0) {
            throw preError;
        }
        try {
            getClickHouseClient().insert(block);
        } catch (Throwable e) {
            handleCanRetry(block, retryTimes - 1, e);
        }
        return true;
    }

    @Override
    public void start() throws Exception {

    }
}
