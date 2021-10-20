package main.java.com.clickhouse_1c_tj;

import main.java.com.clickhouse_1c_tj.config.AppConfig;
import main.java.com.clickhouse_1c_tj.config.ClickHouseConnect;
import ru.yandex.clickhouse.*;

import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;

public class ClickHouseLoader implements Runnable {
    static final Logger logger = LoggerFactory.getLogger(ClickHouseLoader.class);

    // Параметры подключения к Clickhouse
    final ClickHouseConnect chConfig;
    final Map<ClickHouseQueryParam, String> chAdditionalDBParams = new EnumMap<>(ClickHouseQueryParam.class);

    private final Queue<Path> logsPool;
    private final int batchSize;
    private final ClickHouseDataSource dataSource;
    private ClickHouseConnection connection;

    Set<String> setFields = new HashSet<>();
    int processedFiles;
    int processedRecords;

    public ClickHouseLoader(AppConfig config, Queue<Path> logsPool) {
        chConfig = config.clickhouse;
        batchSize = config.getBatchSize();
        this.logsPool = logsPool;

        String url = "jdbc:clickhouse://" + chConfig.getHost()
                + ":" + chConfig.getPort()
                + "/" + chConfig.getDatabase();
        dataSource = new ClickHouseDataSource(url);
        chAdditionalDBParams.put(ClickHouseQueryParam.DATABASE, chConfig.getDatabase());

        processedFiles = 0;
        processedRecords = 0;
    }

    public static void addWatchLogMerge(AppConfig config, Set<String> setFields) {
        String url = "jdbc:clickhouse://" + config.clickhouse.getHost()
                + ":" + config.clickhouse.getPort()
                + "/system";
        StringJoiner joiner = new StringJoiner(", ");
        Map<String, String> defaultFileds = getDefaultColumns();
        for (String field: setFields) {
            String fieldAttr = defaultFileds.getOrDefault(field, "String");
            joiner.add(field + " " + fieldAttr);
        }

        String sql = "CREATE TABLE IF NOT EXISTS WatchLog ("
                + joiner
                + ") ENGINE=Merge(currentDatabase(), '.+_TJ')";
        ClickHouseDataSource tmpDataSource = new ClickHouseDataSource(url);
        try (ClickHouseConnection conn = tmpDataSource.getConnection(config.clickhouse.getUser(), config.clickhouse.getPass());
             ClickHouseStatement stmt = conn.createStatement()) {
            stmt.executeQuery(sql);
        } catch (SQLException e) {
            logger.error("Не удалось выполнить запрос к базе ClickHouse {}", url);
            e.printStackTrace();
        }

    }

    @Override
    public void run() {
        logger.info("Запущен поток #{}", Thread.currentThread().getName());
        
        // Пока в пуле есть необработанные логи, выполняем их парсинг и загрузку
        while (!logsPool.isEmpty()) {
            Path logFile = logsPool.poll();
            if (logFile == null) 
                break; // Другой поток уже мог успеть взять последний файл в проработку

            try {
                // Создаем парсер лога и выполняем загрузку
                load(new TechJournalParser(logFile));
            } catch (FileNotFoundException e) {
                logger.error("Не удалось загрузить файл ТЖ {}", logFile.toAbsolutePath());
                e.printStackTrace();
            } catch (SQLException e) {
                logger.error("Не удалось выполнить запрос к базе Clickhouse: {}", e.toString());
                e.printStackTrace();
            }
        }
    }

    public static boolean checkDB (AppConfig config, boolean createDB) {
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
                }
                else
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

    public void load(TechJournalParser parser) throws SQLException {
        if (parser.isEmpty()) {
            logger.info("Файл пустой {}", parser.pathToLog.toAbsolutePath());
            return;
        }
        processedFiles++;

        String tablename = getTablename(parser);
        // Минимальный набор колонок для таблицы
        SortedMap<String, String> defaultColumns = getDefaultColumns();
        // Пополняемое множество итоговых имен колонок таблицы (пополняется перед загрузкой пакета)
        setFields.addAll(defaultColumns.keySet());
        LogRecord lastRecord;
        if (tableExist(tablename)) {
            // Если таблица существует, то получим ее описание, обновим список колонок, при необходимости добавим отсутствующие
            updateExistingTableBeforeLoading(tablename, defaultColumns, setFields);
            // Получим последнюю запись в логе
            lastRecord = getLastRecord(tablename, parser.filename, parser.parentName);
            if (lastRecord == null)
                logger.info("Ранее файл {}/{} не загружался", parser.parentName, parser.filename);
            else
                logger.info("Последняя загруженная запись: {}", lastRecord);
        } else {
            createTable(tablename, defaultColumns, setFields);
            lastRecord = null;
        }

        while (!parser.isCompleted()) {
            // Получаем распарсенный лог порциями по batchSize
            List<LogRecord> batchToInsert = parser.getNextRecords(batchSize, lastRecord);
            // Обновим набор колонок в таблице, если в логе появились новые поля
            updateColumnsInTable(parser, tablename, setFields);
            // Вставим пакет в таблицу
            insertBatchOfRecords(tablename, batchToInsert, setFields, parser);
            logger.info("Загружено {} записей из файла {}", batchToInsert.size(), parser.pathToLog.toAbsolutePath());
            processedRecords += batchToInsert.size();
        }
    }

    private String getTablename(TechJournalParser parser) {
        // Имя таблицы, куда будет загружен лог, в формате "210615_Main_TJ"
        return parser.yearMonthDayHour.substring(0, 6) + "_" + chConfig.getTablePostfix() + "_TJ";
    }

    public boolean tableExist(String tablename) throws SQLException {
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

    private void updateExistingTableBeforeLoading(String tablename, SortedMap<String, String> defaultColumns, Set<String> setFields) throws SQLException {
        SortedMap<String, String> existingColumns = getTableDescription(tablename);
        Set<String> missingColumnsNames = new HashSet<>(setFields);
        missingColumnsNames.removeAll(existingColumns.keySet()); // Колонки, которых не хватает в существующей таблице
        SortedMap<String, String> missingColumns = new TreeMap<>();
        missingColumnsNames.forEach(colName -> missingColumns.put(colName, defaultColumns.getOrDefault(colName, "String")));
        addColumns(tablename, missingColumns); // Добавим отсутствующие колонки
        setFields.addAll(existingColumns.keySet()); // Запомним все добавленные колонки
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

    private void updateColumnsInTable(TechJournalParser parser, String tablename, Set<String> setFields) throws SQLException {
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
        for (Map.Entry<String, String> newColumn: newColumns.entrySet()) {
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
        for (String newColumn: newColumns) {
            addColumn(tablename, newColumn, "String");
            joiner.add(newColumn);
        }
        logger.info("В таблицу {} добавлены колонки {}", tablename, joiner);
    }

    private void addColumn(String tablename, String newColumn, String columnProperties) throws SQLException {
        String query = String.format("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s", tablename, newColumn, columnProperties);
        execQuery(query);
    }

    private void createTable(String tablename, Map<String, String> defaultColumns, Set<String> setFields) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(String.format("CREATE TABLE IF NOT EXISTS %s (%n", tablename));
        // Добавим все колонки по умолчанию
        Set<String> missingColumnsNames = new HashSet<>(setFields);
        SortedMap<String, String> missingColumns = new TreeMap<>();
        missingColumnsNames.forEach(colName -> missingColumns.put(colName, defaultColumns.getOrDefault(colName, "String")));
        StringJoiner columnJoiner = new StringJoiner(",\n");
        missingColumns.forEach((k, v) -> columnJoiner.add(k + " " + v));
        stringBuilder.append(columnJoiner);
        // Укажем движок таблицы
        stringBuilder.append(String.format(") ENGINE = %s ", chConfig.getEngine()));
        // Сортировка по отметке времени
        stringBuilder.append(String.format("ORDER BY (%s) ", chConfig.getOrderBy()));
        // Секционирование таблицы по ...
        stringBuilder.append(String.format("PARTITION BY (%s)", chConfig.getPartition()));
        String query = stringBuilder.toString();
        execQuery(query);
        logger.info("Создана таблица {}", tablename);
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

    private static String addSingleQuotes(String filename) {
        return "'" + filename + "'";
    }

    private ClickHouseConnection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            connection = dataSource.getConnection(chConfig.getUser(), chConfig.getPass());
        }
        return connection;
    }

    public void execQuery(String query) throws SQLException {
        try (ClickHouseStatement stmt = getConnection().createStatement();) {
            stmt.executeQuery(query, chAdditionalDBParams);
        } catch (SQLException e) {
            throw new SQLException(String.format("Ошибка при выполнении запроса: %s", query), e);
        }
    }

    private void insertBatchOfRecords(String tablename, List<LogRecord> batchToInsert, Set<String> setFields, TechJournalParser parser) throws SQLException {
        StringJoiner joinerColumns = new StringJoiner(",");
        StringJoiner joinerParams = new StringJoiner(",");
        for (String field: setFields) {
            joinerColumns.add(field);
            joinerParams.add("?");
        }
        String insertQuery = "INSERT INTO "
                + tablename
                + " ("
                + joinerColumns
                + ") VALUES ("
                + joinerParams
                + ")";

        try (PreparedStatement stmt = getConnection().prepareStatement(insertQuery)) {
            for (LogRecord rec: batchToInsert) {
                int i = 1;
                for (String field: setFields) {
                    switch (field) {
                        case "filename":
                            stmt.setString(i, parser.filename);
                            break;
                        case "parent":
                            stmt.setString(i, parser.parentName);
                            break;
                        case "source_pid":
                            stmt.setInt(i, parser.parentPid);
                            break;
                        case "source":
                            stmt.setString(i, parser.source);
                            break;
                        case "datetime":
                            // There is a mistake in ClickHouse-JDBC driver in class ClickHouseValueFormatter:
                            // nano-part concatenates to a parameter value without leading zeros.
                            // This code:
                            // if (time != null && time.getNanos() % 1000000 > 0) {
                            //        formatted.append('.').append(time.getNanos()); ←---- !!!
                            //     }
                            // stmt.setTimestamp(i, Timestamp.valueOf(rec.timestamp));
                            stmt.setString(i, rec.getDateTime64CH());
                            break;
                        case "line_number":
                            stmt.setInt(i, rec.lineNumberInFile);
                            break;
                        case "duration":
                            stmt.setLong(i, rec.duration);
                            break;
                        case "event":
                            stmt.setString(i, rec.event);
                            break;
                        case "level":
                            stmt.setString(i, rec.level);
                            break;
                        default:
                            stmt.setString(i, rec.get(field));
                            break;
                    }
                    i++;
                }
                stmt.addBatch();
            }
            ((ClickHousePreparedStatementImpl) stmt).executeBatch(chAdditionalDBParams);
        }
    }

    private static SortedMap<String, String> getDefaultColumns() {
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

}

