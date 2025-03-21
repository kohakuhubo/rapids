package cn.berry.rapids.data.source;

import cn.berry.rapids.exception.ClosedException;

public interface SourceLoader {

    void load() throws ClosedException, InterruptedException;

}
