package com.clickhouse1ctj.config;

import org.junit.jupiter.api.*;
import sun.misc.Unsafe;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {
    static final String TEST_CONFIG_PATH = "src\\test\\resources\\AppConfig\\test_config.yaml";
    private final ByteArrayOutputStream outputStreamCaptor = new ByteArrayOutputStream();

    @BeforeEach
    void setUp() {
        System.setOut(new PrintStream(outputStreamCaptor));
        System.setErr(new PrintStream(outputStreamCaptor));
    }

    @AfterEach
    void tearDown() {
        System.setOut(System.out);
        System.setErr(System.err);
    }

    @DisplayName("Проверка чтения yaml файла с настройками")
    @Test
    void readConfigFromFile() {
        assertNotNull(
                assertDoesNotThrow(
                        () -> AppConfig.readConfigFromFile(TEST_CONFIG_PATH),
                        "Не удалось прочитать конфигурационный файл: " + TEST_CONFIG_PATH
                ), "После чтения конфигурации объект не должен быть null"
        );
    }

    @DisplayName("Проверка получения объекта с конфигурацией без указания пути к yaml файлу")
    @Test
    void getConfigWithoutPath() {
        assertNotNull(
                assertDoesNotThrow(
                        () -> AppConfig.getConfig(null),
                        "Не удалось получить настройки без указания пути"
                ), "После чтения конфигурации объект не должен быть null"
        );
    }

    @DisplayName("Проверка получения объекта с конфигурацией")
    @Test
    void getConfig() {
        String message = "Не удалось получить конфиг из файла";
        AppConfig testConfig = AppConfig.getConfig(TEST_CONFIG_PATH);
        assertNotNull(testConfig, message);
        assertMatchesOutput(".+Используются настройки из файла.+", message);

    }

    @Nested
    class TestConfigGetter {
        AppConfig testConfig;

        @BeforeEach
        void setUp() {
            testConfig = AppConfig.getConfig(TEST_CONFIG_PATH);
        }

        @Test
        void isDaemonMode() {
            assertFalse(testConfig.isDaemonMode());
        }

        @Test
        void getBatchSize() {
            assertEquals(50, testConfig.getBatchSize());
        }

        @Test
        void getThreadCount() {
            assertEquals(5, testConfig.getThreadCount());
        }

        @Test
        void getLogExtension() {
            assertEquals(".log", testConfig.getLogExtension());
        }

        @Test
        void getThresholdSizeHashByAttr() {
            assertEquals(50000, testConfig.getThresholdSizeHashByAttr());
        }

        @Test
        void getMonitoringIntervalSec() {
            assertEquals(10, testConfig.getMonitoringIntervalSec());
        }
    }

    @Nested
    class TestConfigSetter {
        AppConfig testConfig;

        @BeforeEach
        void setUp() {
            testConfig = AppConfig.getConfig(TEST_CONFIG_PATH);
        }

       @Test
        void setDaemonMode() {
            testConfig.setDaemonMode(true);
            assertTrue(testConfig.isDaemonMode());
        }

        @Test
        void setBatchSize() {
            testConfig.setBatchSize(6666);
            assertEquals(6666, testConfig.getBatchSize());
        }

        @Test
        void setThreadCount() {
            testConfig.setThreadCount(10);
            assertEquals(10, testConfig.getThreadCount());
        }

        @Test
        void setLogExtension() {
            testConfig.setLogExtension(".txt");
            assertEquals(".txt", testConfig.getLogExtension());
        }

        @Test
        void setThresholdSizeHashByAttr() {
            testConfig.setThresholdSizeHashByAttr(100000);
            assertEquals(100000, testConfig.getThresholdSizeHashByAttr());
        }

        @Test
        void setMonitoringIntervalSec() {
            testConfig.setMonitoringIntervalSec(77);
            assertEquals(77, testConfig.getMonitoringIntervalSec());
        }
    }

    @Nested
    class TestConfigWithEnvironment {

        @DisplayName("Использование переменных окружения при построении объекта конфигурации")
        @Test
        void checkEnvVarConf() {
            // В данном методе используется установка переменных окружения, которая приводит к предупреждениям illegalAccess.
            // Чтобы сделать более чистым вывод результатов теста, отключим данные предупреждения
            disableWarning();

            // Запомним текущие переменные окружения, которые собираемся менять
            Map<String, String> prevEnv = new HashMap<>();
            for (ConfigKeys key : ConfigKeys.values()) {
                String val = System.getenv(key.getKey());
                prevEnv.put(key.getKey(), val);
            }

            // Формируем новые значения для тестируемых переменных окружения
            Map<String, String> newEnv = new HashMap<>();
            newEnv.put(ConfigKeys.DAEMON_MODE.getKey(), "true");
            newEnv.put(ConfigKeys.LOG_FILE_EXTENSION.getKey(), ".xxx");
            newEnv.put(ConfigKeys.THREADCOUNT.getKey(), "20");
            newEnv.put(ConfigKeys.BATCHSIZE.getKey(), "555");
            newEnv.put(ConfigKeys.THRESHOLD_SIZE_HASH_BY_ATTR.getKey(), "1000");
            newEnv.put(ConfigKeys.MONITORING_INTERVAL_SEC.getKey(), "1");
            newEnv.put(ConfigKeys.CH_HOST.getKey(), "eHOST");
            newEnv.put(ConfigKeys.CH_PORT.getKey(), "1111");
            newEnv.put(ConfigKeys.CH_USER.getKey(), "eUSER");
            newEnv.put(ConfigKeys.CH_PASS.getKey(), "ePASS");
            newEnv.put(ConfigKeys.CH_DATABASE.getKey(), "eDB");
            newEnv.put(ConfigKeys.CH_ENGINE.getKey(), "eENGINE");
            newEnv.put(ConfigKeys.CH_TABLEPOSTFIX.getKey(), "ePOS");
            newEnv.put(ConfigKeys.CH_ORDERBY.getKey(), "eORDER");
            newEnv.put(ConfigKeys.CH_PARTITION.getKey(), "ePART");
            // Устанавливаем новые значения переменных окружения
            assertDoesNotThrow(()->setEnv(newEnv));

            // Проверяем результат
            AppConfig testEnvConf = AppConfig.getConfig(null);
            assertTrue(testEnvConf.isDaemonMode());
            assertEquals(".xxx", testEnvConf.getLogExtension());
            assertEquals(20, testEnvConf.getThreadCount());
            assertEquals(555, testEnvConf.getBatchSize());
            assertEquals(1000, testEnvConf.getThresholdSizeHashByAttr());
            assertEquals(1, testEnvConf.getMonitoringIntervalSec());
            assertEquals("eHOST", testEnvConf.clickhouse.getHost());
            assertEquals("1111", testEnvConf.clickhouse.getPort());
            assertEquals("eUSER", testEnvConf.clickhouse.getUser());
            assertEquals("ePASS", testEnvConf.clickhouse.getPass());
            assertEquals("eDB", testEnvConf.clickhouse.getDatabase());
            assertEquals("eENGINE", testEnvConf.clickhouse.getEngine());
            assertEquals("ePOS", testEnvConf.clickhouse.getTablePostfix());
            assertEquals("eORDER", testEnvConf.clickhouse.getOrderBy());
            assertEquals("ePART", testEnvConf.clickhouse.getPartition());

            // Откатываем переменные окружения
            assertDoesNotThrow(()->setEnv(prevEnv));
        }

    }

    private void assertMatchesOutput(String pattern, String failMessage) {
        assertTrue(Pattern.matches(pattern,
                        outputStreamCaptor.toString().trim()),
                () -> failMessage
                        +". Вывод в лог: "
                        + outputStreamCaptor.toString().trim()
                        + "\nотличается от ожидаемого: "
                        + pattern);
        try {
            outputStreamCaptor.flush();
        } catch (IOException ignore) {}
    }

    private static void setEnv(Map<String, String> newEnv) throws Exception {
        try {
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
            theEnvironmentField.setAccessible(true);
            Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
            env.putAll(newEnv);
            Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
            theCaseInsensitiveEnvironmentField.setAccessible(true);
            Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
            cienv.putAll(newEnv);
        } catch (NoSuchFieldException e) {
            Class[] classes = Collections.class.getDeclaredClasses();
            Map<String, String> env = System.getenv();
            for(Class cl : classes) {
                if("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
                    Field field = cl.getDeclaredField("m");
                    field.setAccessible(true);
                    Object obj = field.get(env);
                    Map<String, String> map = (Map<String, String>) obj;
                    map.clear();
                    map.putAll(newEnv);
                }
            }
        }
    }

    public static void disableWarning() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            Unsafe u = (Unsafe) theUnsafe.get(null);

            Class cls = Class.forName("jdk.internal.module.IllegalAccessLogger");
            Field logger = cls.getDeclaredField("logger");
            u.putObjectVolatile(cls, u.staticFieldOffset(logger), null);
        } catch (Exception ignore) {}
    }
}