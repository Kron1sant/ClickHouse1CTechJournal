package com.clickhouse1ctj.config;

public enum ConfigKeys {
    PATH_TO_CONFIG,
    DAEMON_MODE("setDaemonMode", boolean.class),
    LOG_FILE_EXTENSION("setLogExtension", String.class),
    THREADCOUNT("setThreadCount", int.class),
    BATCHSIZE("setBatchSize", int.class),
    THRESHOLD_SIZE_HASH_BY_ATTR("setThresholdSizeHashByAttr", int.class),
    MONITORING_INTERVAL_SEC("setMonitoringIntervalSec", int.class),
    CH_HOST("clickhouse.setHost", String.class),
    CH_PORT("clickhouse.setPort", String.class),
    CH_USER("clickhouse.setUser", String.class),
    CH_PASS("clickhouse.setPass", String.class),
    CH_DATABASE("clickhouse.setDatabase", String.class),
    CH_ENGINE("clickhouse.setEngine", String.class),
    CH_TABLEPOSTFIX("clickhouse.setTablePostfix", String.class),
    CH_ORDERBY("clickhouse.setOrderBy", String.class),
    CH_PARTITION("clickhouse.setPartition", String.class);

    private final String setMethodName;
    private final Class<?> valueType;

    ConfigKeys() {
        // Обыкновенный ключ
        this.setMethodName = "";
        this.valueType = null;
    }

    <T> ConfigKeys(String setMethodName, Class<T> valueType) {
        // Ключ с указанием метода установки значения для конфигурации и типа значения
        this.setMethodName = setMethodName;
        this.valueType = valueType;
    }

    public String getKey() {
        return name();
    }

    public String getSetterName() {
        return setMethodName;
    }

    public Class<?> getValueType() {
        return valueType;
    }

    @Override
    public String toString() {
        return name();
    }
}
