package cn.berry.rapids.data.source;

import cn.berry.rapids.CycleLife;
import cn.berry.rapids.NamedThreadFactory;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.Executable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

public abstract class SourceParserGenerator implements CycleLife {

    private ExecutorService executorService;

    private final String tbPrefix;

    private final List<Executable> executables;

    protected Configuration configuration;

    public SourceParserGenerator(String tbPrefix, Configuration configuration) {
        this.tbPrefix = tbPrefix;
        this.configuration = configuration;
        this.executables = new ArrayList<>(4);
    }

    protected void addExecutable(Executable executable) {
        this.executables.add(executable);
    }

    @Override
    public void start() throws Exception {
        int size = this.executables.size();
        if (size > 0) {
            this.executorService = new ThreadPoolExecutor(size, size, 0L, java.util.concurrent.TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(), new NamedThreadFactory(this.tbPrefix));
            for (Executable executable : this.executables) {
                executorService.execute(executable);
            }
        }
    }

    @Override
    public void stop() throws Exception {
        for (Executable executable : this.executables) {
            executable.stop();
        }
        if (this.executorService != null)
            this.executorService.shutdown();
    }
}
