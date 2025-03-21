package cn.berry.rapids.eventbus;

public class EmptyEvent implements Event<String> {

    public static final EmptyEvent INSTANCE = new EmptyEvent();

    @Override
    public String type() {
        return "empty";
    }

    @Override
    public String getMessage() {
        return null;
    }
}
