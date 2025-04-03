package cn.berry.rapids.data.source;

import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.Executable;

/**
 * 抽象数据源解析器类
 * 
 * 描述: 提供数据源解析器的基本实现，负责从数据源中读取数据并进行解析。
 * 
 * 特性:
 * 1. 支持泛型数据类型的解析
 * 2. 提供可配置的解析环境
 * 3. 实现可中断的解析流程
 * 
 * @author Berry
 * @version 1.0.0
 */
public abstract class AbstractSourceParser<T> extends Executable {

    /**
     * 应用配置对象，提供解析器所需的配置信息
     */
    protected final Configuration configuration;

    /**
     * 数据源处理器，负责处理解析后的数据
     */
    protected final SourceProcessor<T> sourceProcessor;

    /**
     * 数据源对象，提供原始数据
     */
    protected final Source<T> source;

    /**
     * 解析数据的具体实现方法
     * 
     * @return 解析是否成功
     */
    protected abstract boolean parse();

    /**
     * 构造抽象数据源解析器
     * 
     * @param configuration 应用配置对象，提供解析器配置
     * @param sourceProcessor 数据源处理器，处理解析后的数据
     * @param source 数据源对象，提供原始数据
     */
    public AbstractSourceParser(Configuration configuration, SourceProcessor<T> sourceProcessor, Source<T> source) {
        this.configuration = configuration;
        this.sourceProcessor = sourceProcessor;
        this.source = source;
    }

    /**
     * 运行解析器
     * 
     * 描述: 在未终止且线程未被中断的情况下，持续解析数据源中的数据
     */
    @Override
    public void run() {
        while (!isTerminal() && !Thread.currentThread().isInterrupted() && source.continuable()) {
            parse();
        }
    }
}