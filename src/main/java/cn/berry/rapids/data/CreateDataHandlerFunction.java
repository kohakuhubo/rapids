package cn.berry.rapids.data;

import cn.berry.rapids.aggregate.AggregateServiceHandler;
import cn.berry.rapids.configuration.Configuration;
import cn.berry.rapids.eventbus.Subscription;
import com.berry.clickhouse.tcp.client.ClickHouseClient;

@FunctionalInterface
public interface CreateDataHandlerFunction {

    Subscription<?> apply(Integer number, Configuration configuration, ClickHouseClient client,
                          AggregateServiceHandler aggregateServiceHandler);

}
