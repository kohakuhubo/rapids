package cn.berry.rapids.data.source;

import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.data.Executable;

public abstract class AbstractSourceParser<T> extends Executable {

    protected final Configuration configuration;

    protected final SourceProcessor<T> sourceProcessor;

    protected final Source<T> source;

    protected abstract boolean parse();

    public AbstractSourceParser(Configuration configuration, SourceProcessor<T> sourceProcessor, Source<T> source) {
        this.configuration = configuration;
        this.sourceProcessor = sourceProcessor;
        this.source = source;
    }

    @Override
    public void run() {
        while (!isTerminal() && !Thread.currentThread().isInterrupted() && source.continuable()) {
            parse();
        }
    }
}
