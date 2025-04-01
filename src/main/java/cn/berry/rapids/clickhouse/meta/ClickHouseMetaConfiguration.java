package cn.berry.rapids.clickhouse.meta;

import com.berry.clickhouse.tcp.client.jdbc.ClickHouseTableMetaData;
import com.berry.clickhouse.tcp.client.misc.CollectionUtil;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ClickHouseMetaConfiguration {

    private final Map<String, ClickHouseTableMetaData> metaDataMap;

    private final MetaData metaData;

    public ClickHouseMetaConfiguration(Map<String, ClickHouseTableMetaData> metaDataMap, MetaData metaData) {
        this.metaDataMap = metaDataMap;
        this.metaData = metaData;
    }

    public static ClickHouseMetaConfiguration create(String path) {
        Yaml yaml = new Yaml(new Constructor(MetaData.class, new LoaderOptions()));
        MetaData loadedMetaData = yaml.load(ClickHouseMetaConfiguration.class.getClassLoader().getResourceAsStream(path));
        Map<String, ClickHouseTableMetaData> metaDataMap = loadedMetaData.getMetas().stream().collect(Collectors.toMap(Meta::getName,
                meta -> new ClickHouseTableMetaData(meta.getName(), meta.getColumnNames(), meta.getColumnTypes())));
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

        private String sourceType;

        private String name;

        private List<String> columnNames;

        private List<String> columnTypes;

        private String calculationClass;

        private String persistenceHandler;

        public String getSourceType() {
            return sourceType;
        }

        public void setSourceType(String sourceType) {
            this.sourceType = sourceType;
        }

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

        public String getPersistenceHandler() {
            return persistenceHandler;
        }

        public void setPersistenceHandler(String persistenceHandler) {
            this.persistenceHandler = persistenceHandler;
        }
    }


    public MetaData getMetaData() {
        return metaData;
    }


}
