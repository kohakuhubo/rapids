package cn.berry.rapids.configuration;

import java.io.File;

public class SystemConstant {

    public static final String FILE_EXTENSION_INI = ".ini";

    public static final String FILE_EXTENSION_TAR_GZ = ".tar.gz";

    public static final String FILE_EXTENSION_ZIP = ".zip";

    public static final String FILE_EXTENSION_ZSTD = ".zstd";

    public static final String FILE_EXTENSION_SEPARATOR = ".";

    public static final String FILE_BASE_STATE_NAME = "base.stat";

    public static final long DATA_ITEM_MAX_BUFFER_SIZE = 15 * 1024 * 1024;

    public static final String AGGREGATE_TYPE_ALL = "all";

    public static final String ID_SEPARATOR = "-";

    public static final String CONFIG_YAML_NAME = "config.yaml";

    public static final String EXTRA_CONFIG_PATH = "conf" + File.separator + CONFIG_YAML_NAME;

    public static final String FILE_NAME_SEPARATOR = "_";
}
