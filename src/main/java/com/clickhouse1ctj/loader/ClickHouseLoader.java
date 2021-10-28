package com.clickhouse1ctj.loader;

import com.clickhouse1ctj.parser.LogRecord;
import com.clickhouse1ctj.parser.TechJournalParser;
import com.clickhouse1ctj.config.AppConfig;
import com.clickhouse1ctj.config.ClickHouseConnect;

import com.clickhouse1ctj.parser.TechJournalParserException;
import ru.yandex.clickhouse.*;
import ru.yandex.clickhouse.settings.ClickHouseQueryParam;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClickHouseLoader implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseLoader.class);

    // Параметры подключения к Clickhouse
    private final ClickHouseConnect chConfig;
    private final Queue<Path> logsPool;
    private final int batchSize;
    private final ClickHouseDataSource dataSource;
    private final Map<ClickHouseQueryParam, String> chAdditionalDBParams = new EnumMap<>(ClickHouseQueryParam.class);
    private ClickHouseConnection connection;
    private int processedFiles; // счетчик обработанных файлов ТЖ
    private int processedRecords; // счетчик обработанных записей ТЖ

    public ClickHouseLoader(AppConfig config, Queue<Path> logsPathsPool) {
        chConfig = config.clickhouse;
        batchSize = config.getBatchSize();
        logsPool = logsPathsPool;

        String url = "jdbc:clickhouse://" + chConfig.getHost()
                + ":" + chConfig.getPort()
                + "/" + chConfig.getDatabase();
        dataSource = new ClickHouseDataSource(url);
        chAdditionalDBParams.put(ClickHouseQueryParam.DATABASE, chConfig.getDatabase());

        processedFiles = 0;
        processedRecords = 0;
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
                logger.debug("Старт загрузки файла {}", logFile.toAbsolutePath());
                // Создаем парсер лога и выполняем загрузку
                load(new TechJournalParser(logFile));
                logger.debug("Завершена загрузка файла {}", logFile.toAbsolutePath());
            } catch (FileNotFoundException | TechJournalParserException e) {
                logger.error("Не удалось загрузить файл ТЖ {}", logFile.toAbsolutePath());
                e.printStackTrace();
            } catch (SQLException e) {
                logger.error("Не удалось выполнить запрос к базе Clickhouse при загрузке из файла {}", logFile.toAbsolutePath());
                e.printStackTrace();
            }
        }
        try {
            closeConnection();
        } catch (SQLException e) {
            logger.error("Не удалось закрыть соединение после окончания загрузки");
            e.printStackTrace();
        }
        logger.info("Поток #{} закончил работу, обработав из {} файлов {} строк",
                Thread.currentThread().getName(), processedFiles, processedRecords);
    }

    public void load(TechJournalParser parser) throws SQLException {
        if (parser.isEmpty()) {
            logger.info("Файл пустой {}. Загрузка не требуется", parser.pathToLog.toAbsolutePath());
            return;
        }
        processedFiles++;

        // Определим имя и подготовим таблицу в БД
        String tablename = getTablename(parser);
        logger.debug("Определена таблица {} для загрузки из файла {}", tablename, parser.pathToLog.toAbsolutePath());
        ClickHouseDDL.prepareTableSync(tablename);
        // Получим последнюю запись в логе (от которой будет продолжена загрузка)
        LogRecord lastRecord = getLastRecord(tablename, parser.filename, parser.parentName);
        if (lastRecord == null)
            logger.info("Ранее файл {}/{} не загружался", parser.parentName, parser.filename);
        else
            logger.info("Последняя загруженная запись: {}", lastRecord);

        while (!parser.isCompleted()) {
            // Получаем распарсенный лог порциями по batchSize
            List<LogRecord> batchToInsert = parser.getNextRecords(batchSize, lastRecord);
            // Обновим набор колонок в таблице, если в логе появились новые поля
            ClickHouseDDL.updateColumnsInTable(tablename, parser.getParsedFields());
            // Вставим пакет в таблицу
            insertBatchOfRecords(tablename, batchToInsert, parser);
            logger.info("Загружено {} записей из файла {}", batchToInsert.size(), parser.pathToLog.toAbsolutePath());
            processedRecords += batchToInsert.size();
        }
    }

    private String getTablename(TechJournalParser parser) {
        // Имя таблицы, куда будет загружен лог, в формате "210615_Main_TJ"
        return parser.yearMonthDayHour.substring(0, 6) + "_" + chConfig.getTablePostfix() + "_TJ";
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

    private void insertBatchOfRecords(String tablename, List<LogRecord> batchToInsert, TechJournalParser parser) throws SQLException {
        StringJoiner joinerColumns = new StringJoiner(",");
        StringJoiner joinerParams = new StringJoiner(",");
        SortedSet<String> setFields = parser.getParsedFields();
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
                            stmt.setInt(i, rec.getLineNumberInFile());
                            break;
                        case "duration":
                            stmt.setLong(i, rec.getDuration());
                            break;
                        case "event":
                            stmt.setString(i, rec.getEvent());
                            break;
                        case "level":
                            stmt.setString(i, rec.getLevel());
                            break;
                        default:
                            stmt.setString(i, rec.get(field));
                            break;
                    }
                    i++;
                }
                stmt.addBatch();
            }
            // Выполним пакетную вставку значений в таблицу
            TableLock.getTableLock(tablename).down(); // Используется семафор, чтобы исключить параллельные операции DDL
            ((ClickHousePreparedStatementImpl) stmt).executeBatch(chAdditionalDBParams);
            logger.debug("Выполнена вставка в таблицу {}. Количество добавляемых строк: {}", tablename, batchToInsert.size());
        } catch (SQLException e) {
            logger.error("Не удалось выполнить запрос: {}. Количество добавляемых строк: {}", insertQuery, batchToInsert.size());
            throw new SQLException("Ошибка при пакетной вставка в таблицу", e);
        } finally {
            TableLock.getTableLock(tablename).up(); // Возврат семафора
        }
    }

    public int getProcessedRecords() {
        return processedRecords;
    }

    public int getProcessedFiles() {
        return processedFiles;
    }

    private static String addSingleQuotes(String filename) {
        return "'" + filename + "'";
    }
}

