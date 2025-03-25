package cn.berry.rapids.clickhouse.meta;

import com.berry.clickhouse.tcp.client.jdbc.ClickHouseTableMetaData;
import com.berry.clickhouse.tcp.client.misc.CollectionUtil;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClickHouseMetaConfiguration {

    private final Map<String, ClickHouseTableMetaData> metaDataMap;

    private final MetaData metaData;

    public ClickHouseMetaConfiguration(Map<String, ClickHouseTableMetaData> metaDataMap, MetaData metaData) {
        this.metaDataMap = metaDataMap;
        this.metaData = metaData;
    }

    public static ClickHouseMetaConfiguration create(String path) {
        Map<String, ClickHouseTableMetaData> metaDataMap = new ConcurrentHashMap<>();
        Yaml yaml = new Yaml(new Constructor(MetaData.class, new LoaderOptions()));
        MetaData loadedMetaData = yaml.load(ClickHouseMetaConfiguration.class.getClassLoader().getResourceAsStream(path));
        for (Meta meta : loadedMetaData.getMetas()) {
            if (meta.isMeta)
                metaDataMap.put(meta.getName(),
                        new ClickHouseTableMetaData(meta.getName(), meta.getColumnNames(), meta.getColumnTypes(),
                                CollectionUtil.isNotEmpty(meta.getSystemBufferColumns()) ? new HashSet<>(meta.getSystemBufferColumns()) : Collections.emptySet()));
        }
        return new ClickHouseMetaConfiguration(metaDataMap, loadedMetaData);
    }

    public static class MetaData {

        private List<Meta> metas;

        public List<Meta> getMetas() {
            return metas;
        }

        public void setMetas(List<Meta> metas) {
            this.metas = metas;
        }
    }

    public static class Meta {

        private String name;

        private List<String> columnNames;

        private List<String> columnTypes;

        private String calculationClass;

        private String consistencyClass;

        private Integer threadSize;

        private Integer insertQueueSize;

        private boolean isMeta;

        private List<String> systemBufferColumns;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        public void setColumnNames(List<String> columnNames) {
            this.columnNames = columnNames;
        }

        public List<String> getColumnTypes() {
            return columnTypes;
        }

        public void setColumnTypes(List<String> columnTypes) {
            this.columnTypes = columnTypes;
        }

        public String getCalculationClass() {
            return calculationClass;
        }

        public void setCalculationClass(String calculationClass) {
            this.calculationClass = calculationClass;
        }

        public String getConsistencyClass() {
            return consistencyClass;
        }

        public void setConsistencyClass(String consistencyClass) {
            this.consistencyClass = consistencyClass;
        }

        public Integer getThreadSize() {
            return threadSize;
        }

        public void setThreadSize(Integer threadSize) {
            this.threadSize = threadSize;
        }

        public boolean isMeta() {
            return isMeta;
        }

        public void setMeta(boolean meta) {
            isMeta = meta;
        }

        public List<String> getSystemBufferColumns() {
            return systemBufferColumns;
        }

        public void setSystemBufferColumns(List<String> systemBufferColumns) {
            this.systemBufferColumns = systemBufferColumns;
        }

        public Integer getInsertQueueSize() {
            return insertQueueSize;
        }

        public void setInsertQueueSize(Integer insertQueueSize) {
            this.insertQueueSize = insertQueueSize;
        }
    }


    public MetaData getMetaData() {
        return metaData;
    }


}
