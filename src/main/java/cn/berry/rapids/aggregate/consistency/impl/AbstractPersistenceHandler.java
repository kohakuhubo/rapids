package cn.berry.rapids.aggregate.consistency.impl;

import cn.berry.rapids.aggregate.consistency.PersistenceHandler;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

public abstract class AbstractPersistenceHandler implements PersistenceHandler {

    private ClickHouseClient client;

    public ClickHouseClient getClient() {
        return client;
    }

    public void setClient(ClickHouseClient client) {
        this.client = client;
    }
}
