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
    // Для всех операций по созданию таблиц, колонок и пр. DDL используется chDDLSync
    static final ClickHouseDDL chDDLSync = ClickHouseDDL.getClickHouseDDLSync();

    // Параметры подключения к Clickhouse
    final ClickHouseConnect chConfig;

    private final Queue<Path> logsPool;
    private final int batchSize;
    private final ClickHouseDataSource dataSource;
    private final Map<ClickHouseQueryParam, String> chAdditionalDBParams = new EnumMap<>(ClickHouseQueryParam.class);
    private ClickHouseConnection connection;

    Set<String> setFields = new HashSet<>(); // колонки-поля таблицы лога ТЖ
    int processedFiles; // счетчик обработанных файлов ТЖ
    int processedRecords; // счетчик обработанных записей ТЖ

    public ClickHouseLoader(AppConfig config, Queue<Path> logsPathsPool) {
        chConfig = config.clickhouse;
        batchSize = config.getBatchSize();
        logsPool = logsPathsPool;
        chDDLSync.setConfig(config);

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

    public void load(TechJournalParser parser) throws SQLException {
        if (parser.isEmpty()) {
            logger.info("Файл пустой {}", parser.pathToLog.toAbsolutePath());
            return;
        }
        processedFiles++;

        String tablename = getTablename(parser);
        // Пополняемое множество итоговых имен колонок таблицы (пополняется перед загрузкой пакета)
        LogRecord lastRecord = chDDLSync.prepareTableSync(tablename, setFields, parser.filename, parser.parentName);

        while (!parser.isCompleted()) {
            // Получаем распарсенный лог порциями по batchSize
            List<LogRecord> batchToInsert = parser.getNextRecords(batchSize, lastRecord);
            // Обновим набор колонок в таблице, если в логе появились новые поля
            chDDLSync.updateColumnsInTable(parser, tablename, setFields);
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

    private void insertBatchOfRecords(String tablename, List<LogRecord> batchToInsert, TechJournalParser parser) throws SQLException {
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
}

