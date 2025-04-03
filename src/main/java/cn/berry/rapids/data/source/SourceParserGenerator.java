package cn.berry.rapids.data.source;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.NamedThreadFactory;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.Executable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 数据源解析器生成器类
 * 
 * 描述: 负责生成和管理数据源解析器，提供解析器的生命周期管理。
 * 
 * 特性:
 * 1. 支持多线程解析
 * 2. 提供解析器的生命周期管理
 * 3. 支持可配置的线程池
 * 
 * @author Berry
 * @version 1.0.0
 */
public abstract class SourceParserGenerator implements CycleLife {

    /**
     * 线程池服务，用于管理解析器线程
     */
    private ExecutorService executorService;

    /**
     * 表前缀，用于生成线程名称
     */
    private final String tbPrefix;

    /**
     * 可执行项列表，存储所有需要执行的解析器
     */
    private final List<Executable> executables;

    /**
     * 应用配置对象
     */
    protected Configuration configuration;

    /**
     * 构造数据源解析器生成器
     * 
     * @param tbPrefix 表前缀，用于生成线程名称
     * @param configuration 应用配置对象
     */
    public SourceParserGenerator(String tbPrefix, Configuration configuration) {
        this.tbPrefix = tbPrefix;
        this.configuration = configuration;
        this.executables = new ArrayList<>(4);
    }

    /**
     * 添加可执行项到解析器列表
     * 
     * @param executable 要添加的可执行项
     */
    protected void addExecutable(Executable executable) {
        this.executables.add(executable);
    }

    /**
     * 启动解析器生成器
     * 
     * 描述: 创建线程池并启动所有解析器
     * 
     * @throws Exception 启动过程中可能发生的任何异常
     */
    @Override
    public void start() throws Exception {
        int size = this.executables.size();
        if (size > 0) {
            this.executorService = new ThreadPoolExecutor(size, size, 0L, java.util.concurrent.TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(), new NamedThreadFactory(this.tbPrefix));
            for (Executable executable : this.executables) {
                executorService.execute(executable);
            }
        }
    }

    /**
     * 停止解析器生成器
     * 
     * 描述: 停止所有解析器并关闭线程池
     * 
     * @throws Exception 停止过程中可能发生的任何异常
     */
    @Override
    public void stop() throws Exception {
        for (Executable executable : this.executables) {
            executable.stop();
        }
        if (this.executorService != null)
            this.executorService.shutdown();
    }
}
