package cn.berry.rapids.configuration;

import cn.berry.rapids.clickhouse.meta.ClickHouseMetaConfiguration;
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

public class Configuration {

    private final ClickHouseMetaConfiguration clickHouseMetaConfiguration;

    private final ClickHouseClientConfig clickHouseClientConfig;

    private final SystemConfig systemConfig;

    public Configuration(ClickHouseMetaConfiguration clickHouseMetaConfiguration, ClickHouseClientConfig clickHouseClientConfig, SystemConfig systemConfig) {
        this.clickHouseMetaConfiguration = clickHouseMetaConfiguration;
        this.clickHouseClientConfig = clickHouseClientConfig;
        this.systemConfig = systemConfig;
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
        ClickHouseMetaConfiguration clickHouseMetaConfiguration = ClickHouseMetaConfiguration.create(systemConfig.getMetaPath());
        return new Configuration(clickHouseMetaConfiguration, clickHouseClientConfig, systemConfig);
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
}
