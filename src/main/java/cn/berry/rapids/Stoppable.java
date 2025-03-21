package cn.berry.rapids;

public class Stoppable {

    protected volatile boolean stopped = false;

    public void stop() {
        this.stopped = true;
    }

    public boolean isTerminal() {
        return this.stopped;
    }
}
