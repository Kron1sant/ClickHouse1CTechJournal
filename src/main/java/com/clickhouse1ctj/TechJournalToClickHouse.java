package com.clickhouse1ctj;

import com.clickhouse1ctj.config.AppConfig;
import com.clickhouse1ctj.loader.ClickHouseDDLer;
import com.clickhouse1ctj.loader.ClickHouseInserter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.cli.*;

public class TechJournalToClickHouse implements Runnable {
    static final Logger logger = LoggerFactory.getLogger(TechJournalToClickHouse.class);

    static AppConfig appConfig;
    // Массив путей, где выполняется поиск логов ТЖ
    static Path[] pathsToLogs;
    // Для хранения найденных фалов используем потокобезопасную очередь
    static final Queue<Path> logsPool = new ConcurrentLinkedQueue<>();
    // В режиме демона будем запоминать просмотренный файлы и их контрольные суммы
    static final Map<Path, byte[]> observedFiles = new HashMap<>();

    public static void main(String[] args) throws ParseException, IOException {
        Options options = new Options();
        options.addOption(new Option("d", "daemon", false, "Daemon mode"));
        options.addOption(new Option("", "config", true, "Config file (*.yaml)"));
        options.addOption(new Option("h", "help", false, "Show help"));

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h")) {
            showHelp(options);
            return;
        }

        // Читаем настройки
        appConfig = AppConfig.getConfig(cmd.getOptionValue("config"));
        if (cmd.hasOption("d")) {
            // Устанавливаем признак службы (демона), только если явно передан ключ,
            // иначе используется настройка полученная при формировании конфига
            appConfig.setDaemonMode(true);
        }

        // Массив путей, в котором будем искать логи
        pathsToLogs = getPaths(cmd.getArgs());

        // В зависимости от режима работы (демон/ не демон) будет
        // запущен исполнитель с интервалом в monitoringIntervalSec или разовый вызов
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        if (appConfig.isDaemonMode()) {
            logger.info("Запущен демон поиска новых логов с интервалом {} секунд", appConfig.getMonitoringIntervalSec());
            executor.scheduleAtFixedRate(new TechJournalToClickHouse(), 0, appConfig.getMonitoringIntervalSec(), TimeUnit.SECONDS);
        } else {
            executor.execute(new TechJournalToClickHouse());
            executor.shutdown();
        }
    }

    static Path[] getPaths(String[] args) throws IOException {
        Path[] paths = new Path[args.length == 0 ? 1 : args.length];
        if (args.length == 0)
            paths[0] = getPath("");
        else
            for (int i = 0; i < args.length; i++)
                paths[i] = getPath(args[i]);

        return paths;
    }

    private static Path getPath(String pathStr) throws IOException {
        Path path = Path.of(pathStr);

        if (!Files.exists(path)) {
            throw new IOException(String.format("Путь к логам не существует: %s", path.toAbsolutePath()));
        }
        return path;
    }

    @Override
    public void run() {
        LocalDateTime startTime = LocalDateTime.now();

        // Выполним проверку подключения
        ClickHouseDDLer.init(appConfig);
        if (!ClickHouseDDLer.checkDB(true)) {
            logger.error("Не удалось подключиться к базе данных Clickhouse. Проверьте параметры подключения " +
                    "(управлять ими можно через переменные окружения). Текущие значения:" +
                    "CH_HOST={}; CH_PORT={}; CH_USER={}; CH_PASS=[forbidden]",
                    appConfig.clickhouse.getHost(),
                    appConfig.clickhouse.getPort(),
                    appConfig.clickhouse.getUser());
            return;
        }

        // Ищем файлы с логами
        if (!findAndPoolingLogFiles()) {
            logger.info("Загрузка отменена: нет файлов для загрузки");
            return;
        }

        // Запускаем указанное в параметрах число потоков по загрузке логов
        List<ClickHouseInserter> loaders = startLoadersExecution();

        // Вывод статистики
        showStatistics(startTime, loaders);
    }

    private static boolean findAndPoolingLogFiles() {
        for (Path pathsToLog : pathsToLogs) {
            logger.info("Инициирована загрузка технологического журнала по адресу {}",
                    pathsToLog.toAbsolutePath());
            try {
                fillLogsPool(pathsToLog);
            } catch (IOException e) {
                logger.warn("Не удалось найти файлы логов по пути {}: {}", pathsToLog, e.getMessage());
                e.printStackTrace();
            }
        }
        return !logsPool.isEmpty();
    }

    private static void fillLogsPool(Path pathToLogs) throws IOException {
        // Ищем все файлы с расширением .log
        try (Stream<Path> streamOfFile = Files.find(pathToLogs.toAbsolutePath(), Integer.MAX_VALUE,
                (p, i) -> p.toString().endsWith(appConfig.getLogExtension()) && Files.isRegularFile(p))) {
            streamOfFile.forEach(
                // Добавим найденный файл в пул для обработки
                TechJournalToClickHouse::addFileToLogsPool
            );
        } catch (IOException e) {
            throw new IOException(String.format("Не удалось прочитать каталог логов %s", pathToLogs.toAbsolutePath()), e);
        }

        if (logsPool.isEmpty())
            logger.info("По пути {} не найдено логов для загрузки", pathToLogs);
        else
            logger.info("По пути {} для загрузки найдено {} логов", pathToLogs, logsPool.size());
    }

    private static void addFileToLogsPool(Path path) {
        if (!appConfig.isDaemonMode()) {
            // В обычном режиме не проверяем изменялись ли файлы, сразу добавим в пул
            logsPool.add(path);
            logger.info("Файл {} добавлен в пул к обработке", path);
            return;
        }

        byte[] previousHash = observedFiles.getOrDefault(path, new byte[0]);
        byte[] currentHash = getFileHash(path);
        if (previousHash.length == 0 || currentHash.length == 0 || Arrays.compare(previousHash, currentHash) != 0) {
            // Файл ранее не обрабатывался или изменился, сохраним текущую контрольную сумму
            observedFiles.put(path, currentHash);
            // B добавим в пул
            logsPool.add(path);
            logger.info("Файл {} добавлен в пул к обработке", path);
        } else
            logger.info("Файл {} не изменился с предыдущей обработки", path);
    }

    private static byte[] getFileHash(Path pathToLog) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            logger.warn("Не удалось получить MD5 хешер: {}", e.getMessage());
            e.printStackTrace();
            return new byte[0];
        }

        boolean overSize;
        try (InputStream is = Files.newInputStream(pathToLog);
             DigestInputStream dis = new DigestInputStream(is, md)) {
            byte[] buf = new byte[8*1024];
            int s;
            int reduceThreshold = appConfig.getThresholdSizeHashByAttr();
            do {
                s = dis.read(buf);
                if (s <= 0) break;
                reduceThreshold -= s;
            } while (reduceThreshold >= 0);
            overSize = reduceThreshold < 0;

        } catch (IOException e) {
            logger.warn("Не удалось получить контрольную сумму файла {}: {}", pathToLog, e.getMessage());
            e.printStackTrace();
            return new byte[0];
        }

        if (overSize) {
            // Если файл имеет размер больше порогового, то полный хеш не получился.
            // Дополнительно к тому что прочитал добавляем атрибуты файла: размер и дату изменения
            ByteBuffer buffer = ByteBuffer.allocate(2*Long.BYTES);
            try {
                buffer.putLong(Files.size(pathToLog)); // Размер файла
                buffer.putLong(Files.getLastModifiedTime(pathToLog).toMillis()); // Последняя правка
            } catch (IOException e) {
                logger.warn("Не удалось получить размер или дату последнего изменения файла {}: {}", pathToLog, e.getMessage());
                e.printStackTrace();
                return new byte[0];
            } catch (BufferOverflowException | ReadOnlyBufferException e) {
                logger.warn("Не удалось получить двоичный буфер по атрибутам файла файла {}: {}", pathToLog, e.getMessage());
                e.printStackTrace();
                return new byte[0];
            }
            md.update(buffer.array());
        }
        return md.digest();
    }

    private static List<ClickHouseInserter> startLoadersExecution() {
        int threadsCount = Integer.min(appConfig.getThreadCount(), logsPool.size());
        logger.info("Загрузка будет выполнена {} потоками", threadsCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);
        List<ClickHouseInserter> loaders = new ArrayList<>();
        for (int i = 0; i < threadsCount; i++) {
            ClickHouseInserter loadThread = new ClickHouseInserter(appConfig, logsPool);
            executor.execute(loadThread);
            loaders.add(loadThread);
        }
        // Ждем завершения всех потоков по загрузке
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS))
                logger.warn("Потоки были завершены по таймауту!");
        } catch (InterruptedException e) {
            logger.error("Ошибка при ожидании завершения потоков: {}", e.getMessage());
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } finally {
            // Закроем общее соединение для операций DDl
            ClickHouseDDLer.close();
        }
        return loaders;
    }

    private static void showStatistics(LocalDateTime startTime, List<ClickHouseInserter> loaders) {
        int totalFiles = 0;
        int totalRecords = 0;
        for (ClickHouseInserter loader: loaders) {
            totalFiles += loader.getProcessedFiles();
            totalRecords += loader.getProcessedRecords();
        }
        Duration duration = Duration.between(startTime, LocalDateTime.now());
        logger.info("Загрузка завершена за {}. Всего из {} непустых файлов загружено {} записей", duration, totalFiles, totalRecords);
    }

    private static void showHelp(Options options) {
        HelpFormatter help = new HelpFormatter();
        help.printHelp("ClickHouse1CTechJournal [OPTIONS] [PATH TO LOG] [PATH TO LOG] ...",
                "If [PATH TO LOG] is omitted then the current directory is used",
                options,
                "For example (start as service), ClickHouse1CTechJournal -d D:\\LOGS\\Full");
    }
}
