package com.clickhouse1ctj.config;

import static com.clickhouse1ctj.config.ConfigKeys.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class AppConfig {
    static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
    private static final String DEFAULT_PATH_TO_CONFIG = "config.yaml";

    public final ClickHouseConnect clickhouse;
    private int threadCount;
    private int batchSize; // количество записей из файла в одной пакетной вставке (INSERT в таблицу)
    private String logExtension; // фильтр расширения для поиска логов
    private int thresholdSizeHashByAttr; // Порог размера файла, когда его контрольная сумма будет считаться по аттрибутам
    private int monitoringIntervalSec; // Интервал поиска файлов для загрузки в режиме демона в секундах

    AppConfig() {
        // Задает настройки по умолчанию
        clickhouse = new ClickHouseConnect();
        setThreadCount(1);
        setBatchSize(1000);
        setLogExtension(".log");
        setThresholdSizeHashByAttr(10*1024*1024);
        setMonitoringIntervalSec(30);
    }

    public static AppConfig getConfig() {
        AppConfig config;
        String pathToConfig = System.getenv(PATH_TO_CONFIG.getKey());
        if (pathToConfig == null) {
            pathToConfig = DEFAULT_PATH_TO_CONFIG;
        }
        try {
            config = AppConfig.readConfigFromFile(pathToConfig);
            logger.info("Используются настройки из файла {}", pathToConfig);
        } catch (AppConfigException e) {
            logger.info("Не удалось прочитать файл с настройками {}, {}", pathToConfig, e.getMessage());
            config = new AppConfig();
            logger.info("Используются настройки по умолчанию");
        }
        config.redefineUsingEnvironmentValues();
        return config;
    }

    public static AppConfig readConfigFromFile(String filename) throws AppConfigException {
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

    private void redefineUsingEnvironmentValues() {
        // Заменяет значения параметров конфигурации из переменных окружения (переменные перечислены в ConfigKeys)
        for (ConfigKeys key:
             ConfigKeys.values()) {
            applySetterFromKey(key.getSetterName(), key.getValueType(), System.getenv(key.getKey()));
        }
    }

    private void applySetterFromKey(String setMethodName, Class<?> parameterType, String value) {
        if (value == null || setMethodName.equals(""))
            return;

        // Ищем вызываемый объект, который отработает указанный метод
        Object invokingObj;
        if (setMethodName.startsWith("clickhouse.")) {
            invokingObj = clickhouse;
            setMethodName = setMethodName.substring("clickhouse.".length());
        } else
            invokingObj = this;

        // Получаем и вызываем метод у объекта с указанными параметрами
        Method method;
        try {
            method = invokingObj.getClass().getMethod(setMethodName, parameterType);
            if (parameterType == int.class)
                method.invoke(invokingObj, Integer.parseInt(value));
            else
                method.invoke(invokingObj, value);
        } catch (SecurityException | IllegalAccessException | NoSuchMethodException | IllegalArgumentException | InvocationTargetException e) {
            logger.debug("Не удалось установить значение параметра конфигурации из переменных окружения: {} {}." +
                    "По причине: {}", setMethodName, value, e.getMessage());
        }
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public String getLogExtension() {
        return this.logExtension;
    }

    public void setLogExtension(String logExtension) {
        this.logExtension = logExtension;
    }

    public int getThresholdSizeHashByAttr() {
        return thresholdSizeHashByAttr;
    }

    public void setThresholdSizeHashByAttr(int thresholdSizeHashByAttr) {
        this.thresholdSizeHashByAttr = thresholdSizeHashByAttr;
    }

    public int getMonitoringIntervalSec() {
        return monitoringIntervalSec;
    }

    public void setMonitoringIntervalSec(int monitoringIntervalSec) {
        this.monitoringIntervalSec = monitoringIntervalSec;
    }
}

