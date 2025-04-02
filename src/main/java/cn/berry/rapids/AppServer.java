package cn.berry.rapids;

import cn.berry.rapids.aggregate.AggregateServer;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.SourceParserServer;
import cn.berry.rapids.data.persistece.BaseDataPersistenceServer;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

/**
 * 应用服务器类
 * 
 * 描述: Rapids应用程序的主服务器类，管理整个应用的生命周期和核心组件。
 * 此类作为应用程序的入口点，负责初始化和协调各个服务组件，包括聚合服务、
 * 数据持久化服务和源数据解析服务。
 * 
 * 特性:
 * 1. 实现生命周期接口，管理应用启动和停止
 * 2. 初始化并管理ClickHouse客户端
 * 3. 协调各个服务组件的启动和停止顺序
 * 4. 监控系统核心数等信息
 * 
 * @author Berry
 * @version 1.0.0
 */
public class AppServer implements CycleLife {

    /** 应用配置对象 */
    private final Configuration configuration;

    /** 聚合服务处理器 */
    private final AggregateServer aggregateServiceHandler;

    /** 基础数据持久化服务 */
    private final BaseDataPersistenceServer baseDataPersistenceServer;

    /** 源数据解析服务 */
    private final SourceParserServer sourceParserServer;

    /** 系统核心数 */
    private final int coreCnt;

    /** 应用启动时间戳 */
    private final long startTimestamp;

    /** ClickHouse客户端 */
    private final ClickHouseClient clickHouseClient;

    /**
     * 构造应用服务器
     * 
     * 详细描述: 初始化应用服务器及其所有组件，包括ClickHouse客户端、
     * 聚合服务、数据持久化服务和源数据解析服务。
     * 
     * @param configuration 应用配置对象
     * @throws Exception 初始化过程中可能发生的任何异常
     */
    public AppServer(Configuration configuration) throws Exception {
        this.clickHouseClient = new ClickHouseClient.Builder()
                .config(configuration.getClickHouseClientConfig()).build();
        this.startTimestamp = System.currentTimeMillis();
        this.coreCnt = Runtime.getRuntime().availableProcessors();
        this.configuration = configuration;
        this.aggregateServiceHandler = new AggregateServer(configuration, this.clickHouseClient);
        this.baseDataPersistenceServer = new BaseDataPersistenceServer(configuration, this.aggregateServiceHandler, this.clickHouseClient);
        this.sourceParserServer = new SourceParserServer(this, configuration, this.baseDataPersistenceServer, this.clickHouseClient);
    }

    /**
     * 获取应用启动时间戳
     * 
     * @return 应用启动的时间戳（毫秒）
     */
    public long getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * 获取系统核心数
     * 
     * @return 系统可用的处理器核心数
     */
    public int getCoreCnt() {
        return coreCnt;
    }

    /**
     * {@inheritDoc}
     * 
     * 启动应用服务器
     * 
     * 详细描述: 按照特定顺序启动各个服务组件：
     * 1. 首先启动聚合服务
     * 2. 然后启动数据持久化服务
     * 3. 最后启动源数据解析服务
     * 
     * @throws Exception 启动过程中可能发生的任何异常
     */
    @Override
    public void start() throws Exception {
        this.aggregateServiceHandler.start();
        this.baseDataPersistenceServer.start();
        this.sourceParserServer.start();
    }

    /**
     * {@inheritDoc}
     * 
     * 停止应用服务器
     * 
     * 详细描述: 按照特定顺序停止各个服务组件：
     * 1. 首先停止源数据解析服务
     * 2. 然后停止数据持久化服务
     * 3. 关闭ClickHouse客户端
     * 4. 最后停止聚合服务
     * 
     * @throws Exception 停止过程中可能发生的任何异常
     */
    @Override
    public void stop() throws Exception {
        this.sourceParserServer.stop();
        this.baseDataPersistenceServer.stop();
        this.clickHouseClient.close();
        this.aggregateServiceHandler.stop();
    }
}
