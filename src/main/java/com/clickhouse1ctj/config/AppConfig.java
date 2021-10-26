package com.clickhouse1ctj.config;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;


public class AppConfig {
    public final ClickHouseConnect clickhouse;
    private int threadsCount;
    private int batchSize;

    AppConfig() {
        clickhouse = new ClickHouseConnect();
        setThreadsCount(1);
        setBatchSize(1000);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getThreadsCount() {
        return threadsCount;
    }

    public void setThreadsCount(int threadsCount) {
        this.threadsCount = threadsCount;
    }

    public static AppConfig readConfig(String filename) throws AppConfigException {
        AppConfig config;
        try (InputStream configStream = new FileInputStream(filename)) {
            Yaml yaml = new Yaml(new Constructor(AppConfig.class));
            config = yaml.load(configStream);
        } catch (FileNotFoundException e) {
            throw new AppConfigException("Файл с настройками config.yaml не обнаружен", e);
        } catch (IOException e) {
            throw new AppConfigException("Не удалось получить настройки из файла config.yaml", e);
        }
        return config;
    }

    @Override
    public String toString() {
        return "ClickHouseConfig{" +
                "clickhouse=" + clickhouse +
                ", threads_count=" + threadsCount +
                ", batch_size=" + batchSize +
                '}';
    }
}

