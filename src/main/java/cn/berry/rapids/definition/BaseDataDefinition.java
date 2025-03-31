package cn.berry.rapids.definition;

import cn.berry.rapids.enums.SourceTypeEnum;

import java.util.Locale;

public class BaseDataDefinition {

    private SourceTypeEnum sourceTypeEnum;

    private String sourceType;

    private String sourceName;

    private String tableName;

    private ColumnDataDefinition[] columnDataDefinitions;

    public String getSourceType() {
        return sourceType;
    }

    public SourceTypeEnum getSourceTypeEnum() {
        return sourceTypeEnum;
    }

    public void setSourceType(String sourceType) {
        this.sourceType = sourceType;
        this.sourceTypeEnum = SourceTypeEnum.valueOf(sourceType.toUpperCase(Locale.ROOT));
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public ColumnDataDefinition[] getColumnDataDefinitions() {
        return columnDataDefinitions;
    }

    public void setColumnDataDefinitions(ColumnDataDefinition[] columnDataDefinitions) {
        this.columnDataDefinitions = columnDataDefinitions;
    }
}
