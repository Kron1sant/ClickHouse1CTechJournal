package com.clickhouse1ctj.loader;

import com.clickhouse1ctj.config.AppConfig;
import com.clickhouse1ctj.config.ClickHouseConnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/* Для всех операций по созданию таблиц, колонок и пр. DDL используется данный класс */
public class ClickHouseDDL {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseDDL.class);
    private static final ClickHouseDDL chDDLSync = new ClickHouseDDL();
    private static ClickHouseConnect chConfig;

    private final Map<ClickHouseQueryParam, String> chAdditionalDBParams = new EnumMap<>(ClickHouseQueryParam.class);
    private ClickHouseDataSource dataSource;
    private ClickHouseConnection connection;
    // Будем хранить кешированный набор колонок по каждой таблице
    private final Map<String, SortedSet<String>> fieldsInTables = new HashMap<>();

    private ClickHouseDDL() {}

    public static void init(AppConfig appConfig) {
        // Используем синглтон для синхронных операций по изменению схемы базы данных
        synchronized (chDDLSync) {
            chConfig = appConfig.clickhouse;
            String url = "jdbc:clickhouse://" + chConfig.getHost()
                    + ":" + chConfig.getPort()
                    + "/" + chConfig.getDatabase();
            chDDLSync.dataSource = new ClickHouseDataSource(url);
            chDDLSync.chAdditionalDBParams.put(ClickHouseQueryParam.DATABASE, chConfig.getDatabase());
        }
    }

    public static boolean checkDB(boolean createDB) {
        String url = "jdbc:clickhouse://" + chConfig.getHost()
                + ":" + chConfig.getPort()
                + "/system";
        String sql = "SHOW DATABASES LIKE '" + chConfig.getDatabase() + "'";
        ClickHouseDataSource tmpDataSource = new ClickHouseDataSource(url);
        try (ClickHouseConnection conn = tmpDataSource.getConnection(chConfig.getUser(), chConfig.getPass());
             ClickHouseStatement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (!rs.next()) {
                if (createDB) {
                    String sqlCreateDB = "CREATE DATABASE " + chConfig.getDatabase();
                    stmt.executeQuery(sqlCreateDB);
                    logger.info("Создана новая база данных {}", chConfig.getDatabase());
                    return true;
                } else
                    throw new SQLException("Нет указанной базы данных " + chConfig.getDatabase());
            } else
                return true;
       } catch (SQLException e) {
            logger.error("Не удалось выполнить запрос к базе ClickHouse {}", url);
            e.printStackTrace();
            return false;
       }
    }

    public static void prepareTableSync(String tablename) throws SQLException {
        synchronized (TableLock.getTableLock(tablename)) {
            TableLock.getTableLock(tablename).check();
            logger.debug("Подготовка таблицы {} для загрузки", tablename);
            SortedSet<String> setExistFields = getFieldsInTable(tablename);
            if (chDDLSync.tableExist(tablename)) {
                logger.debug("Таблица {} существует", tablename);
                // Если таблица существует, то получим ее описание, обновим список колонок, при необходимости добавим отсутствующие
                chDDLSync.updateExistingTableBeforeLoading(tablename, setExistFields);
            } else {
                logger.debug("Таблица {} не существует. Будет создана новая", tablename);
                // Иначе создаем новую таблицу
                chDDLSync.createTable(tablename, setExistFields);
            }
        }
    }

    public static void updateColumnsInTable(String tablename, SortedSet<String> setParsedFields) throws SQLException {
        synchronized (TableLock.getTableLock(tablename)) {
            TableLock.getTableLock(tablename).check();
            SortedSet<String> setExistFields = getFieldsInTable(tablename); // Закешированные поля таблицы
            Set<String> setNewColumns = new HashSet<>(setParsedFields); // Поля полученные при парсинге лога (копируем, так как будем изменять)
            logger.debug("Обновление колонок таблицы {} при загрузке очередного пакета. Состав известных колонок {}. " +
                    "Состав колонок из пакета {}", tablename, setExistFields, setNewColumns);
            setNewColumns.removeAll(setExistFields); // Поля, которые отсутствуют в таблице
            chDDLSync.addColumns(tablename, setNewColumns); // Добавим новые колонки
            setExistFields.addAll(setNewColumns); // Сохраним новые колонки в коллекции
        }
    }

    public static void close() {
        try {
            chDDLSync.closeConnection();
        } catch (SQLException e) {
            logger.error("Не удалась закрыть соединение ClickHouseDDL: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    private static SortedSet<String> getFieldsInTable(String tablename) {
        SortedSet<String> setFields = chDDLSync.fieldsInTables.get(tablename);
        if (setFields == null) {
            logger.debug("Для таблицы {} ранее не был закеширован набор полей", tablename);
            setFields = new TreeSet<>();
            chDDLSync.fieldsInTables.put(tablename, setFields);
        } else {
            logger.debug("Для таблицы {} получен закешированный набор полей {}", tablename, setFields);
        }
        return setFields;
    }

    private boolean tableExist(String tablename) throws SQLException {
        String query = String.format("EXISTS TABLE %s", tablename);
        try (ClickHouseStatement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(query, chAdditionalDBParams)) {
            if (rs.next()) {
                return rs.getString(1).equals("1");
            } else {
                return false;
            }
        }
    }

    private void updateExistingTableBeforeLoading(String tablename, Set<String> setFields) throws SQLException {
        // Получим уже существующие колонки в таблице
        SortedMap<String, String> existingColumns = getTableDescription(tablename);
        logger.debug("В таблице {} присутствуют следующие колонки {}", tablename, existingColumns);
        // Проверим состав закешированных полей, если они пусты, то добавим в кэш существующие поля и выйдем из процедуры
        if (setFields.isEmpty()) {
            logger.debug("Запрашиваемый набор колонок для добавления в таблицу {} пуст. Будет запомнен набор существующих в таблице колонок", tablename);
            setFields.addAll(existingColumns.keySet());
            return;
        }
        // Получим имена колонок-полей, которые были получены в ходе анализа логов ТЖ
        Set<String> missingColumnsNames = new HashSet<>(setFields);
        // Вычтем из списка те колонки, которые уже существуют
        missingColumnsNames.removeAll(existingColumns.keySet());
        logger.debug("Для добавления в таблицу {} определен следующий набор колонок {}", tablename, missingColumnsNames);
        // Соберем итоговый набор колонок для добавлений
        SortedMap<String, String> defaultColumns = getDefaultColumns();
        SortedMap<String, String> missingColumns = new TreeMap<>();
        missingColumnsNames.forEach(colName -> missingColumns.put(colName, defaultColumns.getOrDefault(colName, "String")));
        // Добавим колонки к таблице
        addColumns(tablename, missingColumns);
        // Запомним все добавленные колонки
        setFields.addAll(missingColumnsNames);
    }

    private SortedMap<String, String> getTableDescription(String tablename) throws SQLException {
        String query = "DESCRIBE TABLE " + chConfig.getDatabase() + "." + tablename;
        SortedMap<String, String> columns = new TreeMap<>();
        try (ClickHouseStatement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                columns.put(rs.getString(1), rs.getString(2));
            }
        }
        return columns;
    }

    private void addColumns(String tablename, SortedMap<String, String> newColumns) throws SQLException {
        if (newColumns.size() == 0) {
            return;
        }

        StringJoiner joiner = new StringJoiner(", ");
        for (Map.Entry<String, String> newColumn : newColumns.entrySet()) {
            addColumn(tablename, newColumn.getKey(), newColumn.getValue());
            joiner.add(newColumn.getKey());
        }
        logger.info("В таблицу {} добавлены колонки {}", tablename, joiner);
    }

    private void addColumns(String tablename, Set<String> newColumns) throws SQLException {
        if (newColumns.isEmpty()) {
            logger.debug("В таблицу {} нет колонок для добавления", tablename);
            return;
        }

        StringJoiner joiner = new StringJoiner(", ");
        for (String newColumn : newColumns) {
            addColumn(tablename, newColumn, "String");
            joiner.add(newColumn);
        }
        logger.info("В таблицу {} добавлены колонки {}", tablename, joiner);
    }

    private void addColumn(String tablename, String newColumn, String columnProperties) throws SQLException {
        String query = String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", tablename, newColumn, columnProperties);
        logger.debug("SQL-запрос на добавление колонки {}", query);
        execQuery(query);
    }

    private void createTable(String tablename, Set<String> setFields) throws SQLException {
        StringBuilder query = new StringBuilder();
        query.append(String.format("CREATE TABLE IF NOT EXISTS %s (%n", tablename));
        // Добавим все колонки по умолчанию
        SortedMap<String, String> defaultColumns = getDefaultColumns();
        logger.debug("Для таблицы {} закеширован набор колонок: {}", tablename, setFields);
        setFields.addAll(defaultColumns.keySet());
        // Получим список отсутствующих колонок для таблицы
        SortedMap<String, String> missingColumns = new TreeMap<>();
        setFields.forEach(colName -> missingColumns.put(colName, defaultColumns.getOrDefault(colName, "String")));
        // Соберем строку с описанием колонок для SQL запроса
        StringJoiner columnJoiner = new StringJoiner(",\n");
        missingColumns.forEach((k, v) -> columnJoiner.add(k + " " + v));
        // Добавим описание колонок в основной текст запроса создания таблицы
        query.append(columnJoiner);
        // Укажем движок таблицы
        query.append(String.format(") ENGINE = %s ", chConfig.getEngine()));
        // Сортировка по отметке времени
        query.append(String.format("ORDER BY (%s) ", chConfig.getOrderBy()));
        // Секционирование таблицы по ...
        query.append(String.format("PARTITION BY (%s)", chConfig.getPartition()));
        execQuery(query.toString());
        logger.info("Создана таблица {}", tablename);
        logger.debug("SQL запрос на создание таблицы {}", query);
    }

    private void execQuery(String query) throws SQLException {
        try (ClickHouseStatement stmt = getConnection().createStatement()) {
            stmt.executeQuery(query, chAdditionalDBParams);
        } catch (SQLException e) {
            throw new SQLException(String.format("Ошибка при выполнении запроса: %s", query), e);
        }
    }

    private ClickHouseConnection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = dataSource.getConnection(chConfig.getUser(), chConfig.getPass());
        }
        return connection;
    }

    private void closeConnection() throws SQLException {
        if (connection != null && !connection.isClosed())
            connection.close();
    }

    public static SortedMap<String, String> getDefaultColumns() {
        SortedMap<String, String> defaultColumns = new TreeMap<>();
        defaultColumns.put("filename", "String");
        defaultColumns.put("parent", "String");
        defaultColumns.put("source", "String");
        defaultColumns.put("source_pid", "UInt32");
        defaultColumns.put("line_number", "UInt32");
        defaultColumns.put("datetime", "DateTime(6)");
        defaultColumns.put("duration", "UInt64");
        defaultColumns.put("event", "String");
        defaultColumns.put("level", "String");
        return defaultColumns;
    }

    public boolean checkTableColumns(String tablename, Set<String> setFields) throws SQLException {
        // TEST.
        SortedMap<String, String> existingColumns = getTableDescription(tablename);
        Set<String> copyFields = new TreeSet<>(setFields);
        copyFields.removeAll(existingColumns.keySet());
        if (copyFields.isEmpty()) {
            return true;
        } else {
            logger.error("В таблице отсутствуют колонки {} ", copyFields);
            return false;
        }
    }
}

