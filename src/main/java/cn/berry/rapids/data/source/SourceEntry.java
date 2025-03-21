package cn.berry.rapids.data.source;

public interface SourceEntry<T> {

    long id();

    T entry();

    void success();

    void fail();

}
