package cn.berry.rapids.configuration;

import cn.berry.rapids.clickhouse.meta.ClickHouseMetaConfiguration;
import cn.berry.rapids.definition.BaseDataDefinition;
import cn.berry.rapids.definition.BaseDataDefinitions;
import cn.berry.rapids.enums.SourceTypeEnum;
import com.berry.clickhouse.tcp.client.buffer.StringTypeCacheBufferPoolManager;
import com.berry.clickhouse.tcp.client.data.StringTypeColumnWriterBufferPoolManager;
import com.berry.clickhouse.tcp.client.settings.ClickHouseClientConfig;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class Configuration {

    private final ClickHouseMetaConfiguration clickHouseMetaConfiguration;

    private final ClickHouseClientConfig clickHouseClientConfig;

    private final SystemConfig systemConfig;

    private final Map<SourceTypeEnum, Map<String, BaseDataDefinition>> sourceBaseDataDefinitionMap;

    private final List<BaseDataDefinition> baseDataDefinitions;

    private final static String BASE_DATA_YAML_PATH = "./conf/baseData.yaml";

    private final static String META_YAML_PATH = "./conf/meta.yaml";

    public Configuration(ClickHouseMetaConfiguration clickHouseMetaConfiguration, ClickHouseClientConfig clickHouseClientConfig,
                         List<BaseDataDefinition> baseDataDefinitions, SystemConfig systemConfig) {

        Map<SourceTypeEnum, Map<String, BaseDataDefinition>> sourceBaseDataDefinitionMap = baseDataDefinitions.stream()
                .collect(java.util.stream.Collectors.groupingBy(BaseDataDefinition::getSourceTypeEnum,
                        java.util.stream.Collectors.toMap(BaseDataDefinition::getSourceName, baseDataDefinition -> baseDataDefinition)));

        this.clickHouseMetaConfiguration = clickHouseMetaConfiguration;
        this.clickHouseClientConfig = clickHouseClientConfig;
        this.systemConfig = systemConfig;
        this.sourceBaseDataDefinitionMap = sourceBaseDataDefinitionMap;
        this.baseDataDefinitions = baseDataDefinitions;
    }

    public static Configuration createConfiguration(String path) throws UnsupportedEncodingException, FileNotFoundException {
        URL url = Configuration.class.getProtectionDomain().getCodeSource().getLocation();
        String filePath = URLDecoder.decode(url.getPath(), "utf-8");
        filePath = filePath.substring(0, filePath.lastIndexOf(File.separator) + 1);

        InputStream inputStream;
        File file = new File(filePath + path);
        if (file.exists()) {
            inputStream = new FileInputStream(file);
        } else {
            inputStream = Configuration.class.getResourceAsStream(SystemConstant.CONFIG_YAML_NAME);
        }
        Yaml yaml = new Yaml(new Constructor(SystemConstant.class, new LoaderOptions()));
        SystemConfig systemConfig = yaml.load(inputStream);

        // 读取 baseDataDefinitions
        Yaml baseDataYaml = new Yaml(new Constructor(BaseDataDefinitions.class, new LoaderOptions()));
        BaseDataDefinitions dataDefinitions = baseDataYaml.load(Configuration.class.getResourceAsStream(BASE_DATA_YAML_PATH));
        List<BaseDataDefinition> baseDataDefinitions = dataDefinitions.baseDataDefinitions();

        ClickHouseConfig clickHouseConfig = systemConfig.getClickHouse();
        BlockConfig blockConfig = systemConfig.getBlock();
        ClickHouseClientConfig clickHouseClientConfig = ClickHouseClientConfig
                .Builder.builder()
                //clickhouse数据库配置
                .host(clickHouseConfig.getHost()).port(9000)
                .user(clickHouseConfig.getUser()).password(clickHouseConfig.getPassword())
                .database(clickHouseConfig.getDatabase())
                .connectTimeout(Duration.ofMillis(clickHouseConfig.getConnectTimeout()))
                .queryTimeout(Duration.ofMillis(clickHouseConfig.getQueryTimeout()))
                //clickhouse连接池
                .connectionPoolMaxIdle(clickHouseConfig.getConnectionMaxIdle())
                .connectionPooMinIdle(clickHouseConfig.getConnectionMinIdle())
                .connectionPoolTotal(clickHouseConfig.getConnectionTotal())
                //String类型列缓存池管理器
                .columnWriterBufferPoolManager(new StringTypeColumnWriterBufferPoolManager(blockConfig.getStackSize(),
                        blockConfig.getSelfBufferSize(), blockConfig.getStringStackSize(), blockConfig.getStringSelfBufferSize()))
                //String类型缓存池管理器
                .bufferPoolManager(new StringTypeCacheBufferPoolManager(blockConfig.getBlockSize(),
                        blockConfig.getStringBlockSize(), blockConfig.getCacheLength()))
                .build();
        ClickHouseMetaConfiguration clickHouseMetaConfiguration = ClickHouseMetaConfiguration.create(META_YAML_PATH);

        return new Configuration(clickHouseMetaConfiguration, clickHouseClientConfig, baseDataDefinitions, systemConfig);
    }

    public ClickHouseMetaConfiguration getClickHouseMetaConfiguration() {
        return clickHouseMetaConfiguration;
    }

    public ClickHouseClientConfig getClickHouseClientConfig() {
        return clickHouseClientConfig;
    }

    public SystemConfig getSystemConfig() {
        return systemConfig;
    }

    public BaseDataDefinition getBaseDataDefinition(SourceTypeEnum sourceTypeEnum, String sourceName) {
        Map<String, BaseDataDefinition> baseDataDefinitions = this.sourceBaseDataDefinitionMap.get(sourceTypeEnum);
        if (baseDataDefinitions == null) {
            return null;
        }
        return baseDataDefinitions.get(sourceName);
    }

    public List<BaseDataDefinition> getBaseDataDefinitions() {
        return baseDataDefinitions;
    }
}
