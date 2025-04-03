package cn.berry.rapids.aggregate.calculation;

import cn.berry.rapids.eventbus.BlockDataEvent;
import com.berry.clickhouse.tcp.client.data.Block;

public interface CalculationHandler {

    String id();

    String type();

    Block handle(BlockDataEvent block);

}
