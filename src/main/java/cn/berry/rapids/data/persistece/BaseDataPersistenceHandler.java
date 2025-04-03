package cn.berry.rapids.data.persistece;

import cn.berry.rapids.eventbus.Event;
import cn.berry.rapids.eventbus.Subscription;

public interface BaseDataPersistenceHandler<T> extends Subscription<Event<T>> {

    void handle(T baseData);

}
