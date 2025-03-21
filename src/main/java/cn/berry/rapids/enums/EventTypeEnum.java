package cn.berry.rapids.enums;

public enum EventTypeEnum {

    FILE_PARSE("file_parse"),
    DATA_INSERT("data_insert"),
    DATA_CALCULATION("data_calculation"),
    AGGREGATE_INSERT("aggregate_insert"),;

    private final String type;

    EventTypeEnum(String type) {
        this.type = type;
    }
    public String getType() {
        return type;
    }

}
