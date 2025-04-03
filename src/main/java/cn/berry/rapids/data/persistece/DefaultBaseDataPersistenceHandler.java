package cn.berry.rapids.data.persistece;

import cn.berry.rapids.aggregate.AggregateServiceHandler;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.BlockEvent;
import cn.berry.rapids.model.BaseData;
import com.berry.clickhouse.tcp.client.ClickHouseClient;
import com.berry.clickhouse.tcp.client.data.Block;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultBaseDataPersistenceHandler extends AbstractBaseDataPersistenceHandler {

    private static final Logger logger = LoggerFactory.getLogger(DefaultBaseDataPersistenceHandler.class);

    private final int retryTimes;

    @Override
    protected void flush() {

    }

    public DefaultBaseDataPersistenceHandler(Configuration configuration, AggregateServiceHandler aggregateServiceHandler, ClickHouseClient clickHouseClient) {
        super(configuration, aggregateServiceHandler, clickHouseClient);
        int retryTimes = configuration.getSystemConfig().getData().getDataInsertRetryTimes();
        if (retryTimes <= 0) {
            retryTimes = 3;
        }
        this.retryTimes = retryTimes;
    }

    /**
     * 处理区块事件
     *
     * @param baseData 基础数据
     * @return 是否处理成功
     */
    @Override
    public void handle(BaseData baseData) {
        boolean res = false;
        try {
            res = handleCanRetry(baseData.getBlock(), retryTimes, null);
        } catch (Throwable e) {
            logger.error("handle block has error! retry times:" + retryTimes, e);
        }
        //如果处理成功，提交到聚合服务处理器
        if (res) {
            AggregateServiceHandler aggregateServiceHandler = getAggregateServiceHandler();
            if (aggregateServiceHandler != null) {
                aggregateServiceHandler.handle(new BlockEvent(baseData.type(), baseData.getBlock()));
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
    public String id() {
        return "default";
    }

    @Override
    public String type() {
        return "base";
    }
}
