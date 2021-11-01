package com.clickhouse1ctj;

import com.clickhouse1ctj.config.AppConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.io.IOException;
import java.nio.file.Path;
import java.security.cert.TrustAnchor;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TechJournalToClickHouseTest {
    static final String TEST_DATABASE_NAME = "TEST_DATABASE_FOR_JUNIT";
    static AppConfig testConfig;
    private ClickHouseDataSource tmpDataSource;

    @BeforeAll
    static void beforeAll() {
        testConfig = new AppConfig();
        testConfig.setThreadCount(2);
        testConfig.setDaemonMode(false);
        testConfig.clickhouse.setTablePostfix("test");
        testConfig.clickhouse.setDatabase(TEST_DATABASE_NAME);
    }

    @Test
    void getPaths() throws IOException {
        Path[] paths = TechJournalToClickHouse.getPaths(
                new String[] {"src/test/resources/test_logs/rphost_1234", "src/test/resources/test_logs/ragent_4321"});
        assertEquals(2, paths.length);
        assertEquals("rphost_1234", paths[0].getFileName().toString());
        assertEquals("ragent_4321", paths[1].getFileName().toString());
    }

    @Test
    @EnabledIf("com.clickhouse1ctj.TechJournalToClickHouseTest#isClickHouseEnabled")
    void run() throws SQLException {
        // Подготавливаем параметры тестовой среды для запуска загрузки
        TechJournalToClickHouse.appConfig = testConfig;
        TechJournalToClickHouse.pathsToLogs = new Path[]{
                Path.of("src/test/resources/test_logs/rphost_1234"),
                Path.of("src/test/resources/test_logs/ragent_4321")};

        // Инициируем загрузка
        TechJournalToClickHouse testApp = new TechJournalToClickHouse();
        testApp.run();

        // Проверяем результат загрузки
        // Число записей в ТЖ
        String logsTablename = TEST_DATABASE_NAME + ".211022_test_TJ";
        int testCountOfRecordsInLogs = 40;
        assertEquals(testCountOfRecordsInLogs, getCountOfRecords(logsTablename));

        // Число записей "Событие-Свойство"
        String eventPropsTablename = TEST_DATABASE_NAME + ".properties_by_events_tj";
        int testCountOfRecordsInEventProps = 140;
        assertEquals(testCountOfRecordsInEventProps, getCountOfRecords(eventPropsTablename));

        // Конкретные параметры эталонного события из ТЖ
        String filename = "21102215.log"; // Ключ поиска
        String parent = "rphost_1234"; // Ключ поиска
        int lineNumber = 24; // Ключ поиска
        String fieldsStr = "datetime,duration,event,OSThread,Sql,Context"; // Возвращаемые поля
        Map<String, String> testLogRecord = getRecord(fieldsStr, logsTablename, filename, parent, lineNumber);
        assert testLogRecord != null;
        assertEquals("2021-10-22 15:20:19.957001", testLogRecord.get("datetime"));
        assertEquals("3", testLogRecord.get("duration"));
        assertEquals("DBMSSQL", testLogRecord.get("event"));
        assertEquals("9952", testLogRecord.get("OSThread"));
        assertEquals("SELECTT1._IDRRef,T1._CodeFROM dbo._Acc11 T1WHERE ((T1._Fld14598 = ?)) AND (T1._IDRRef = ?)p_0: 0Np_1: 0x10BF8BA9C77FDAED4C8B400ADC26680E", testLogRecord.get("Sql"));
        assertEquals(2700, testLogRecord.get("Context").length());

        // Чистим за собой
        dropDB();
        TechJournalToClickHouse.appConfig = null;
        TechJournalToClickHouse.pathsToLogs = null;
    }

    boolean isClickHouseEnabled() {
        String url = "jdbc:clickhouse://" + testConfig.clickhouse.getHost()
                + ":" + testConfig.clickhouse.getPort()
                + "/system";
        String sql = "SHOW DATABASES LIKE '" + TEST_DATABASE_NAME + "'";
        ClickHouseProperties properties = new ClickHouseProperties();
        properties.setSocketTimeout(3000); // Чтобы сильно не удлинять тест, если ClickHouse отключен - поставим таймаут в 3 секунды
        tmpDataSource = new ClickHouseDataSource(url, properties);
        try (ClickHouseConnection conn = tmpDataSource.getConnection(testConfig.clickhouse.getUser(), testConfig.clickhouse.getPass());
             ClickHouseStatement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                // Если тестовая база есть, удалим ее
                dropDB();
            }
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private int getCountOfRecords(String tablename) {
        String sql = "SELECT count(*) FROM " + tablename;
        try (ClickHouseConnection conn = tmpDataSource.getConnection(
                testConfig.clickhouse.getUser(), testConfig.clickhouse.getPass());
             ClickHouseStatement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            return rs.next() ? rs.getInt(1): 0;
        } catch (SQLException e) {
            return -1;
        }
    }

    private Map<String, String> getRecord(String fieldsStr, String tablename, String filename, String parent, int lineNumber) {
        List<String> fields = List.of(fieldsStr.split(","));
        Map<String, String> logRecord = new HashMap<>();
        String sql = "SELECT "
                + fieldsStr
                + " FROM "
                + tablename
                + " WHERE filename='"
                + filename
                + "' AND parent='"
                + parent
                + "' AND line_number="
                + lineNumber;
        try (ClickHouseConnection conn = tmpDataSource.getConnection(
                testConfig.clickhouse.getUser(), testConfig.clickhouse.getPass());
             ClickHouseStatement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                for (String field : fields) {
                    logRecord.put(field, rs.getString(field));
                }
            } else
                return null;
        } catch (SQLException e) {
            return null;
        }
        return logRecord;
    }

    private void dropDB() throws SQLException {
        String url = "jdbc:clickhouse://" + testConfig.clickhouse.getHost()
                + ":" + testConfig.clickhouse.getPort()
                + "/system";
        String sql = "DROP DATABASE IF EXISTS " + TEST_DATABASE_NAME;
        ClickHouseDataSource tmpDataSource = new ClickHouseDataSource(url);
        try (ClickHouseConnection conn = tmpDataSource.getConnection(testConfig.clickhouse.getUser(), testConfig.clickhouse.getPass());
             ClickHouseStatement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            // pass
        } catch (SQLException e) {
            throw new SQLException(String.format("Не удалось удалить тестовую базу данных %s", TEST_DATABASE_NAME));
        }
    }
}