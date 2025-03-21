package cn.berry.rapids.data.source.kafka;

import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.persistece.BaseDataPersistenceServer;
import cn.berry.rapids.data.source.SourceEntry;
import cn.berry.rapids.data.source.SourceProcessor;

public class KafkaSourceProcessor implements SourceProcessor<KafkaSourceEntry> {

    private final Configuration configuration;

    private final BaseDataPersistenceServer baseDataPersistenceServer;

    public KafkaSourceProcessor(Configuration configuration, BaseDataPersistenceServer baseDataPersistenceServer) {
        this.configuration = configuration;
        this.baseDataPersistenceServer = baseDataPersistenceServer;
    }

    @Override
    public void process(SourceEntry<KafkaSourceEntry> entry) {

    }
}
