package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.source.AbstractSourceParser;
import cn.berry.rapids.data.source.Source;
import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.data.source.SourceProcessor;

/**
 * Kafka数据源解析器类
 * 
 * 描述: 负责解析从Kafka消费者中读取的数据，并将其转换为区块事件。
 * 此类实现了生命周期接口，管理解析器的启动和停止。
 * 
 * 特性:
 * 1. 解析Kafka消费者读取的数据
 * 2. 将数据转换为区块事件
 * 
 * @author Berry
 * @version 1.0.0
 */
public class KafkaSourceParser extends AbstractSourceParser<KafkaSourceEntry> {

    /**
     * 构造Kafka数据源解析器
     * 
     * @param configuration 应用配置对象
     * @param sourceProcessor 数据源处理器
     * @param source 数据源
     */
    public KafkaSourceParser(Configuration configuration, SourceProcessor<KafkaSourceEntry> sourceProcessor,
                             Source<KafkaSourceEntry> source) {
        super(configuration, sourceProcessor, source);
    }

    /**
     * 解析数据
     * 
     * @return 是否解析成功
     */
    @Override
    protected boolean parse() {
        SourceEntry<KafkaSourceEntry> entry = source.next();
        if (null == entry)
            return false;
        try {
            this.sourceProcessor.process(entry);
        } catch (Throwable e) {
            //error ifo
        }
        return false;
    }
}
