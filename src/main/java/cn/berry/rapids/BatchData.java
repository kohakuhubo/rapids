package cn.berry.rapids;

public interface BatchData {

    long startRowId();

    long endRowId();

    String sourceMajorId();

    String sourceMinorId();

    int status();

    long dataStartTime();

    long dataEndTime();

    String[] aggregateTypes();

}
