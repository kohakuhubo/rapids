package cn.berry.rapids.data;

import cn.berry.rapids.AppServer;
import cn.berry.rapids.CycleLife;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.persistece.BaseDataPersistenceServer;
import cn.berry.rapids.data.source.SourceParserGenerator;
import cn.berry.rapids.data.source.kafka.KafkaSourceParserGenerator;

import java.util.Collections;
import java.util.List;

public class SourceParserServer implements CycleLife {

    private final AppServer appServer;

    private final Configuration configuration;

    private final BaseDataPersistenceServer baseDataPersistenceServer;

    private final List<SourceParserGenerator> sourceParserGenerators;

    public SourceParserServer(AppServer appServer, Configuration configuration, BaseDataPersistenceServer baseDataPersistenceServer) {
        this.appServer = appServer;
        this.configuration = configuration;
        this.baseDataPersistenceServer = baseDataPersistenceServer;
        this.sourceParserGenerators = Collections.singletonList(new KafkaSourceParserGenerator(appServer, configuration, baseDataPersistenceServer));
    }

    @Override
    public void start() throws Exception {
        for (SourceParserGenerator sourceParserGenerator : this.sourceParserGenerators) {
            sourceParserGenerator.start();
        }
    }

    @Override
    public void stop() throws Exception {
        for (SourceParserGenerator sourceParserGenerator : this.sourceParserGenerators) {
            sourceParserGenerator.stop();
        }
    }
}
