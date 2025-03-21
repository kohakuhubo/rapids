package cn.berry.rapids.data.source;

public interface Source<T> {

    boolean continuable();

    SourceEntry<T> next();

    void commit(SourceEntry<T> entry);

}
