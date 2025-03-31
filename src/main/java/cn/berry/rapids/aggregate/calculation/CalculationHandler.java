package cn.berry.rapids.aggregate.calculation;

import cn.berry.rapids.eventbus.BlockEvent;
import com.berry.clickhouse.tcp.client.data.Block;

public interface CalculationHandler {

    String id();

    String type();

    Block handle(BlockEvent block);

}
