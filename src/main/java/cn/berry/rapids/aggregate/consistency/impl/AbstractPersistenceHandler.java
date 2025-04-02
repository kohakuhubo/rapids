package cn.berry.rapids.aggregate.consistency.impl;

import cn.berry.rapids.aggregate.consistency.PersistenceHandler;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

/**
 * 抽象持久化处理器
 * 
 * 描述: 提供持久化处理器的基础实现，管理ClickHouse客户端。
 * 
 * 特性:
 * 1. 提供对ClickHouse客户端的访问
 * 
 * @author Berry
 * @version 1.0.0
 */
public abstract class AbstractPersistenceHandler implements PersistenceHandler {

    private ClickHouseClient client;

    /**
     * 获取ClickHouse客户端
     * 
     * @return ClickHouse客户端
     */
    public ClickHouseClient getClient() {
        return client;
    }

    /**
     * 设置ClickHouse客户端
     * 
     * @param client ClickHouse客户端
     */
    public void setClient(ClickHouseClient client) {
        this.client = client;
    }
}
