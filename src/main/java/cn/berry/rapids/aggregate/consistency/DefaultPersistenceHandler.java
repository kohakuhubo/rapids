package cn.berry.rapids.aggregate.consistency;

import cn.berry.rapids.Stoppable;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.BlockEvent;
import cn.berry.rapids.eventbus.Subscription;
import com.berry.clickhouse.tcp.client.ClickHouseClient;
import com.berry.clickhouse.tcp.client.data.Block;

public class DefaultPersistenceHandler extends Stoppable implements Subscription<BlockEvent>, PersistenceHandler {

    private final String id;

    private final ClickHouseClient clickHouseClient;

    private final int retryTimes;

    public DefaultPersistenceHandler(String id, ClickHouseClient clickHouseClient, Configuration configuration) {
        this.id = id;
        this.clickHouseClient = clickHouseClient;
        int retryTimes = configuration.getSystemConfig().getAggregate().getInsertRetryTimes();
        if (retryTimes <= 0) {
            retryTimes = 3;
        }
        this.retryTimes = retryTimes;
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public String type() {
        return "default";
    }

    @Override
    public void onMessage(BlockEvent event) {
        handle(event);
    }

    @Override
    public boolean handle(BlockEvent event) {
        if (isTerminal()) {
            return false;
        }
        try {
            handleCanRetry(event.getMessage(), retryTimes, null);
        } catch (Throwable e) {

        }
        return false;
    }

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

    @Override
    public void start() throws Exception {

    }
}
