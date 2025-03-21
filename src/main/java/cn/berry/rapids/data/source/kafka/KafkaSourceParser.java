package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.AppServer;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.source.AbstractSourceParser;
import cn.berry.rapids.data.source.Source;
import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.data.source.SourceProcessor;

public class KafkaSourceParser extends AbstractSourceParser<KafkaSourceEntry> {

    public KafkaSourceParser(Configuration configuration, SourceProcessor<KafkaSourceEntry> sourceProcessor,
                             Source<KafkaSourceEntry> source) {
        super(configuration, sourceProcessor, source);
    }

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
