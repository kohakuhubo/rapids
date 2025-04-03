package cn.berry.rapids.data.persistece;

import cn.berry.rapids.model.SourceDataEvent;

public interface SourceDataPersistenceHandler {

    void handle(SourceDataEvent event);

}
