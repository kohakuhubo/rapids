package cn.berry.rapids.aggregate;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class DataAggregateState {

    private List<String> errorAggregateTypes = new CopyOnWriteArrayList<>();

    private Set<String> aggregateTypes;

    private AtomicInteger successCount = new AtomicInteger(0);

    private final long total;

    public DataAggregateState(long total, Set<String> aggregateTypes) {
        this.total = total;
        this.aggregateTypes = aggregateTypes;
    }

    public long successCount() {
        return successCount.longValue();
    }

    public boolean incrementSuccessCount() {
        long count = successCount.incrementAndGet();
        return (count > 0) && (count == total);
    }

    public void incrementErrorCount(String aggregateType) {
        errorAggregateTypes.add(aggregateType);
    }

    public List<String> getErrorAggregateTypes() {
        return errorAggregateTypes;
    }

    public boolean hasError() {
        long successCnt = successCount.longValue();
        long errorCnt = errorAggregateTypes.size();
        return (errorCnt > 0) && (successCnt > 0) && (this.total == (errorCnt + successCnt));
    }

    public boolean allError() {
        long errorCnt = errorAggregateTypes.size();
        return (errorCnt > 0) && (this.total == errorCnt);
    }

    public Set<String> getAggregateTypes() {
        return aggregateTypes;
    }
}
