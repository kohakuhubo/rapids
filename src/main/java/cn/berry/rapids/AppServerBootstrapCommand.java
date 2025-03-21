package cn.berry.rapids;

import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.configuration.SystemConstant;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(mixinStandardHelpOptions = true, version = "0.1", helpCommand = true)
public class AppServerBootstrapCommand implements Callable<Integer> {

    @CommandLine.Option(names = {"-pts", "parseThreadSize"}, defaultValue = "parse thread size")
    private Integer parseThreadSize;

    @CommandLine.Option(names = {"-dits", "dataInsertThreadSize"}, defaultValue = "data insert thread size")
    private Integer dataInsertThreadSize;

    @CommandLine.Option(names = {"-ats", "aggregateThreadSize"}, defaultValue = "aggregate calculation thread size")
    private Integer aggregateThreadSize;

    @CommandLine.Option(names = {"-diql", "dataInsertQueueLength"}, defaultValue = "data insert queue size")
    private Integer dataInsertQueueLength;

    @CommandLine.Option(names = {"-awql", "aggregateWaitQueueLength"}, defaultValue = "aggregate wait queue length")
    private Integer aggregateWaitQueueLength;

    @CommandLine.Option(names = {"-bdmrc", "batchDataMaxRowCnt"}, defaultValue = "batch data max row count")
    private Integer batchDataMaxRowCnt;

    @CommandLine.Option(names = {"-dbmbs", "batchDataMaxByteSize"}, defaultValue = "data byte size of batch")
    private Integer batchDataMaxByteSize;

    @CommandLine.Option(names = {"-bbfcs", "byteBufferFixedCacheSize"}, defaultValue = "fixed cache size of block")
    private Integer byteBufferFixedCacheSize;

    @CommandLine.Option(names = {"-bbdcs", "byteBufferDynamicCacheSize"}, defaultValue = "dynamic cache size of block")
    private Integer byteBufferDynamicCacheSize;

    @Override
    public Integer call() throws Exception {
        Configuration configuration = Configuration.createConfiguration(SystemConstant.EXTRA_CONFIG_PATH);
        AppServer appServer = new AppServer(configuration);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                appServer.stop();
            } catch(Throwable e) {
                //error info
            }
        }));
        appServer.start();
        return CommandConstant.SUCCESS;
    }
}
