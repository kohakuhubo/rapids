package cn.berry.rapids.eventbus;

public interface EventHandover {

    boolean tryPost(Event<?> t);

    boolean post(Event<?> t, long waitTime) throws InterruptedException;

    Event<?> get() throws InterruptedException;

}
