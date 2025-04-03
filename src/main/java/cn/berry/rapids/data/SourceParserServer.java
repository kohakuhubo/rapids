package cn.berry.rapids.data;

import cn.berry.rapids.AppServer;
import cn.berry.rapids.CycleLife;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.persistece.SourceDataPersistenceServer;
import cn.berry.rapids.data.source.SourceParserGenerator;
import cn.berry.rapids.data.source.kafka.KafkaSourceParserGenerator;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

import java.util.Collections;
import java.util.List;

/**
 * 数据源解析器服务器类
 * 
 * 描述: 负责管理数据源解析器的生命周期，提供解析器的启动和停止功能。
 * 
 * 特性:
 * 1. 支持配置管理
 * 2. 提供生命周期管理
 * 
 * @author Berry
 * @version 1.0.0
 */
public class SourceParserServer implements CycleLife {

    private final AppServer appServer;
/**
     * 应用配置对象
     */
    private final Configuration configuration;

    private final SourceDataPersistenceServer sourceDataPersistenceServer;

    private final List<SourceParserGenerator> sourceParserGenerators;

    /**
     * 构造数据源解析器服务器
     * 
     * @param appServer 应用服务器类
     * @param configuration 应用配置对象
     * @param sourceDataPersistenceServer 基础数据持久化处理器
     * @param clickHouseClient clickhouse客户端客户端
     */
    public SourceParserServer(AppServer appServer, Configuration configuration, SourceDataPersistenceServer sourceDataPersistenceServer,
                              ClickHouseClient clickHouseClient) {
        this.appServer = appServer;
        this.configuration = configuration;
        this.sourceDataPersistenceServer = sourceDataPersistenceServer;
        this.sourceParserGenerators = Collections.singletonList(new KafkaSourceParserGenerator(appServer, configuration, sourceDataPersistenceServer, clickHouseClient));
    }

    /**
     * 启动解析器服务器
     * 
     * @throws Exception 启动过程中可能发生的任何异常
     */
    @Override
    public void start() throws Exception {
        for (SourceParserGenerator sourceParserGenerator : this.sourceParserGenerators) {
            sourceParserGenerator.start();
        }
    }

    /**
     * 停止解析器服务器
     * 
     * @throws Exception 停止过程中可能发生的任何异常
     */
    @Override
    public void stop() throws Exception {
        for (SourceParserGenerator sourceParserGenerator : this.sourceParserGenerators) {
            sourceParserGenerator.stop();
        }
    }
}
