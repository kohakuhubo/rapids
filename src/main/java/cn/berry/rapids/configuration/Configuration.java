package cn.berry.rapids.configuration;

import cn.berry.rapids.clickhouse.meta.ClickHouseMetaConfiguration;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.*;
import java.net.URL;
import java.net.URLDecoder;
import java.time.Duration;

public class Configuration {

    private final ClickHouseMetaConfiguration clickHouseMetaConfiguration;

    private final ClickHouseConfig clickHouseConfig;

    private final SystemConfig systemConfig;

    public Configuration(ClickHouseMetaConfiguration clickHouseMetaConfiguration, ClickHouseConfig clickHouseConfig, SystemConfig systemConfig) {
        this.clickHouseMetaConfiguration = clickHouseMetaConfiguration;
        this.clickHouseConfig = clickHouseConfig;
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
        com.github.housepower.settings.ClickHouseConfig clickHouseClientConfig = com.github.housepower.settings.ClickHouseConfig.Builder.builder()
                .host(clickHouseConfig.getHost()).port(9000)
                .user(clickHouseConfig.getUser()).password(clickHouseConfig.getPassword())
                .database(clickHouseConfig.getDatabase())
                .connectTimeout(Duration.ofMillis(clickHouseConfig.getConnectTimeout()))
                .queryTimeout(Duration.ofMillis(clickHouseConfig.getQueryTimeout()))
                .connectionPoolMaxIdle(clickHouseConfig.getConnectionMaxIdle())
                .connectionPooMinIdle(clickHouseConfig.getConnectionMinIdle())
                .connectionPoolTotal(clickHouseConfig.getConnectionTotal())
                .selfByteBufferLength(blockConfig.getByteBufferFixedCacheLength())
                .selfByteBufferSize(blockConfig.getByteBufferFixedCacheSize())
                .selfColumStackLength(blockConfig.getColumnFixedStackLength())
                .systemByteBufferLength(blockConfig.getByteBufferDynamicCacheLength())
                .systemByteBufferSize(blockConfig.getByteBufferDynamicCacheSize())
                .systemByteBufferStackLength(blockConfig.getByteBufferDynamicCacheStackLength())
                .systemColumStackLength(blockConfig.getColumnDynamicStackLength())
                .build();
        ClickHouseMetaConfiguration clickHouseMetaConfiguration = ClickHouseMetaConfiguration.create(systemConfig.getMetaPath());
        return new Configuration(clickHouseMetaConfiguration, clickHouseConfig, systemConfig);
    }

    public ClickHouseMetaConfiguration getClickHouseMetaConfiguration() {
        return clickHouseMetaConfiguration;
    }

    public ClickHouseConfig getClickHouseConfig() {
        return clickHouseConfig;
    }

    public SystemConfig getSystemConfig() {
        return systemConfig;
    }
}
