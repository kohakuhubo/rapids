package cn.berry.rapids.data.source;

public interface SourceProcessor<T> {

    void process(SourceEntry<T> entry);

}
