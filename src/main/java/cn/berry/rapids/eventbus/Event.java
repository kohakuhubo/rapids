package cn.berry.rapids.eventbus;

public interface Event<T> {

    String type();

    T getMessage();

    default boolean hasMessage() {
        return null != getMessage();
    }

}
