package cn.berry.rapids.eventbus;

public interface Subscription<T> {

    String id();

    String type();

    void onMessage(T event);

}
