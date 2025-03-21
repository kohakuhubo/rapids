package cn.berry.rapids.aggregate.calculation;

import cn.berry.rapids.aggregate.AggregateEntry;
import cn.berry.rapids.aggregate.DataWrapper;

public interface CalculationHandler {

    String id();

    String type();

    AggregateEntry handle(DataWrapper dataWrapper);

}
