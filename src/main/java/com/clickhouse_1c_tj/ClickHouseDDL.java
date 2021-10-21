package main.java.com.clickhouse_1c_tj;

import main.java.com.clickhouse_1c_tj.config.AppConfig;
import main.java.com.clickhouse_1c_tj.config.ClickHouseConnect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHouseConnection;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHouseStatement;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

class ClickHouseDDL {
    static final Logger logger = LoggerFactory.getLogger(ClickHouseDDL.class);
    private static ClickHouseDDL chDDLSync;

    private ClickHouseConnect chConfig;
    private final Map<ClickHouseQueryParam, String> chAdditionalDBParams = new EnumMap<>(ClickHouseQueryParam.class);
    private ClickHouseDataSource dataSource;
    private ClickHouseConnection connection;

    public static ClickHouseDDL getClickHouseDDLSync() {
        if (chDDLSync == null)
            chDDLSync = new ClickHouseDDL();
        return chDDLSync;
    }

    public static boolean checkDB(AppConfig config, boolean createDB) {
        String url = "jdbc:clickhouse://" + config.clickhouse.getHost()
                + ":" + config.clickhouse.getPort()
                + "/system";
        String sql = "SHOW DATABASES LIKE '" + config.clickhouse.getDatabase() + "'";
        ClickHouseDataSource tmpDataSource = new ClickHouseDataSource(url);
        try (ClickHouseConnection conn = tmpDataSource.getConnection(config.clickhouse.getUser(), config.clickhouse.getPass());
             ClickHouseStatement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            if (!rs.next()) {
                if (createDB) {
                    String sqlCreateDB = "CREATE DATABASE " + config.clickhouse.getDatabase();
                    stmt.executeQuery(sqlCreateDB);
                    logger.info("Создана новый база данных {}", config.clickhouse.getDatabase());
                    return true;
                } else
                    throw new SQLException("Нет указанной базы данных " + config.clickhouse.getDatabase());
            } else {
                return true;
            }
        } catch (SQLException e) {
            logger.error("Не удалось выполнить запрос к базе ClickHouse {}", url);
            e.printStackTrace();
            return false;
        }
    }

    public synchronized void setConfig(AppConfig config) {
        chConfig = config.clickhouse;
        String url = "jdbc:clickhouse://" + chConfig.getHost()
                + ":" + chConfig.getPort()
                + "/" + chConfig.getDatabase();
        dataSource = new ClickHouseDataSource(url);
        chAdditionalDBParams.put(ClickHouseQueryParam.DATABASE, chConfig.getDatabase());
    }

    public synchronized LogRecord prepareTableSync(String tablename, Set<String> setFields, String filename, String parentName) throws SQLException {
        LogRecord lastRecord;

       if (tableExist(tablename)) {
            // Если таблица существует, то получим ее описание, обновим список колонок, при необходимости добавим отсутствующие
            updateExistingTableBeforeLoading(tablename, setFields);
            // Получим последнюю запись в логе
            lastRecord = getLastRecord(tablename, filename, parentName);
            if (lastRecord == null)
                logger.info("Ранее файл {}/{} не загружался", parentName, filename);
            else
                logger.info("Последняя загруженная запись: {}", lastRecord);
        } else {
            createTable(tablename, setFields);
            lastRecord = null;
        }
        return lastRecord;
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
        // Если список полей в лоадере пустой, значит он только запустился, обновлять в таблице нечего,
        // запомним существующие табличные поля
        if (setFields.isEmpty()) {
            setFields.addAll(existingColumns.keySet());
            return;
        }
        // Получим имена колонок-полей, которые были получены в ходе анализа логов ТЖ
        Set<String> missingColumnsNames = new HashSet<>(setFields);
        // Вычтем из списка те колонки, которые уже существуют
        missingColumnsNames.removeAll(existingColumns.keySet());
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

    public synchronized void updateColumnsInTable(TechJournalParser parser, String tablename, Set<String> setFields) throws SQLException {
        Set<String> setColumns = new HashSet<>(Set.copyOf(parser.logFields));
        setColumns.removeAll(setFields); // Получим новые колонки
        addColumns(tablename, setColumns); // Добавим новые колонки
        setFields.addAll(setColumns); // Сохраним новые колонки в коллекции
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
        execQuery(query);
    }

    private LogRecord getLastRecord(String tablename, String filename, String parent) throws SQLException {
        // Последняя запись определяется максимальный номером строки в файле, а не отметкой времени, так как
        // встречаются логи ТЖ, где предыдущие записи могут быть старше (на микросекунды) относительно следующих строк
        String query = "SELECT TOP 1 datetime, duration, event, level, line_number FROM "
                + tablename
                + " WHERE filename = "
                + addSingleQuotes(filename)
                + " AND parent = "
                + addSingleQuotes(parent)
                + " ORDER BY line_number DESC";
        try (ClickHouseStatement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(query, chAdditionalDBParams)) {
            if (rs.next()) {
                return new LogRecord(rs.getString(1),
                        rs.getLong(2),
                        rs.getString(3),
                        rs.getString(4),
                        rs.getInt(5));
            } else {
                return null;
            }
        }
    }

    private void createTable(String tablename, Set<String> setFields) throws SQLException {
        StringBuilder query = new StringBuilder();
        query.append(String.format("CREATE TABLE IF NOT EXISTS %s (%n", tablename));
        // Добавим все колонки по умолчанию
        SortedMap<String, String> defaultColumns = getDefaultColumns();
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

    private static String addSingleQuotes(String filename) {
        return "'" + filename + "'";
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
