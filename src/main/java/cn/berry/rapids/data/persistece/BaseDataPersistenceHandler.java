package cn.berry.rapids.data.persistece;

public interface BaseDataPersistenceHandler<T> {

    void handle(T baseData);

}
