package cn.berry.rapids;

import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.configuration.SystemConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.util.concurrent.Callable;

/**
 * 应用服务器引导命令类
 * 
 * 描述: 应用程序的命令行入口点，负责解析命令行参数并启动应用服务器。
 * 此类使用picocli框架解析命令行参数，并将参数应用于应用配置。
 * 
 * 特性:
 * 1. 实现Callable接口，用于命令行执行
 * 2. 使用picocli注解定义命令行选项
 * 3. 管理应用服务器的生命周期
 * 4. 注册JVM关闭钩子，确保应用优雅退出
 * 
 * @author Berry
 * @version 0.1
 */
@CommandLine.Command(mixinStandardHelpOptions = true, version = "0.1", helpCommand = true)
public class AppServerBootstrapCommand implements Callable<Integer> {

    /** 日志记录器 */
    private static final Logger logger = LoggerFactory.getLogger(AppServerBootstrapCommand.class);

    /** 解析线程数 */
    @CommandLine.Option(names = {"-pts", "parseThreadSize"}, defaultValue = "parse thread size")
    private Integer parseThreadSize;

    /** 数据插入线程数 */
    @CommandLine.Option(names = {"-dits", "dataInsertThreadSize"}, defaultValue = "data insert thread size")
    private Integer dataInsertThreadSize;

    /** 聚合计算线程数 */
    @CommandLine.Option(names = {"-ats", "aggregateThreadSize"}, defaultValue = "aggregate calculation thread size")
    private Integer aggregateThreadSize;

    /** 数据插入队列长度 */
    @CommandLine.Option(names = {"-diql", "dataInsertQueueLength"}, defaultValue = "data insert queue size")
    private Integer dataInsertQueueLength;

    /** 聚合等待队列长度 */
    @CommandLine.Option(names = {"-awql", "aggregateWaitQueueLength"}, defaultValue = "aggregate wait queue length")
    private Integer aggregateWaitQueueLength;

    /** 批处理数据最大行数 */
    @CommandLine.Option(names = {"-bdmrc", "batchDataMaxRowCnt"}, defaultValue = "batch data max row count")
    private Integer batchDataMaxRowCnt;

    /** 批处理数据最大字节大小 */
    @CommandLine.Option(names = {"-dbmbs", "batchDataMaxByteSize"}, defaultValue = "data byte size of batch")
    private Integer batchDataMaxByteSize;

    /** 字节缓冲区固定缓存大小 */
    @CommandLine.Option(names = {"-bbfcs", "byteBufferFixedCacheSize"}, defaultValue = "fixed cache size of block")
    private Integer byteBufferFixedCacheSize;

    /** 字节缓冲区动态缓存大小 */
    @CommandLine.Option(names = {"-bbdcs", "byteBufferDynamicCacheSize"}, defaultValue = "dynamic cache size of block")
    private Integer byteBufferDynamicCacheSize;

    /**
     * 命令执行方法
     * 
     * 详细描述: 创建应用配置，初始化并启动应用服务器。
     * 添加JVM关闭钩子，确保应用在JVM关闭时能够优雅停止。
     * 
     * @return 命令执行结果代码，成功为0
     * @throws Exception 应用启动过程中可能发生的任何异常
     */
    @Override
    public Integer call() throws Exception {
        // 创建应用配置
        Configuration configuration = Configuration.createConfiguration(SystemConstant.EXTRA_CONFIG_PATH);
        
        // 初始化应用服务器
        AppServer appServer = new AppServer(configuration);
        
        // 添加JVM关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                appServer.stop();
            } catch(Throwable e) {
                logger.error("stop error.", e);
            }
        }));
        
        // 启动应用服务器
        appServer.start();
        
        return CommandConstant.SUCCESS;
    }
}
