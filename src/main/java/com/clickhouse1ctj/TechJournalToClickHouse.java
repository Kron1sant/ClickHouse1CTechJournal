package main.java.com.clickhouse1ctj;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

import main.java.com.clickhouse1ctj.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TechJournalToClickHouse {
    static final Logger logger = LoggerFactory.getLogger(TechJournalToClickHouse.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        run(getPath(args));
    }

    private static Path getPath(String[] args) throws IOException {
        Path path;
        if (args.length == 1)
            path = Path.of(args[0]);
        else
            path = Path.of("");

        if (!Files.exists(path)) {
            throw new IOException(String.format("Путь к логам не существует: %s", path.toAbsolutePath()));
        }
        return path;
    }

    public static void run (Path pathToLogs) throws InterruptedException, IOException {
        LocalDateTime startTime = LocalDateTime.now();
        logger.info(" Инициирована загрузка технологического журнала по адресу {}", pathToLogs.toAbsolutePath());

        // Для хранения найденных фалов используем потокобезопасную очередь
        Queue<Path> logsPool = fillLogsPool(pathToLogs);
        if (logsPool.isEmpty())
            return;

        // Чтение настроек
        AppConfig config = AppConfig.readConfig("config.yaml");

        // Выполним проверку подключения
        if (!ClickHouseDDL.checkDB(config, true)) {
            logger.error("Не удалось подключиться к базе данных Clickhouse");
            return;
        }

        // Запускаем указанное в параметрах число потоков по загрузке логов
        int threadsCount = Integer.min(config.getThreadsCount(), logsPool.size());
        logger.info("Загрузка будет выполнена {} потоками", threadsCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);
        List<ClickHouseLoader> loaders = new ArrayList<>();
        for (int i = 0; i < threadsCount; i++) {
            ClickHouseLoader loadThread = new ClickHouseLoader(config, logsPool);
            executor.execute(loadThread);
            loaders.add(loadThread);
        }
        // Ждем завершения всех потоков по загрузке
        executor.shutdown();
        try {
            if (!executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS))
                logger.error("Потоки были завершены по таймауту!");
        } catch (InterruptedException e) {
            logger.error("Ошибка при ожидании завершения потоков");
            throw e;
        }

        // Подсчет статистики
        int totalFiles = 0;
        int totalRecords = 0;
        for (ClickHouseLoader loader: loaders) {
            totalFiles+=loader.processedFiles;
            totalRecords+=loader.processedRecords;
        }
        Duration duration = Duration.between(startTime, LocalDateTime.now());
        logger.info("Загрузка завершена за {}. Всего из {} непустых файлов загружено {} записей", duration, totalFiles, totalRecords);
    }

    private static Queue<Path> fillLogsPool(Path pathToLogs) throws IOException {
        // Очередь будет использована несколькими потоками
        Queue<Path> logsPool = new ConcurrentLinkedQueue<>();

        // Ищем все файлы с расширением .log
        try (Stream<Path> streamOfFile = Files.find(pathToLogs.toAbsolutePath(), Integer.MAX_VALUE,
                (p, i) -> p.toString().endsWith(".log") && Files.isRegularFile(p))) {
            streamOfFile.forEach(logsPool::add);
        } catch (IOException e) {
            throw new IOException(String.format("Не удалось прочитать каталог логов %s", pathToLogs.toAbsolutePath()), e);
        }

        if (logsPool.isEmpty())
            logger.info("Не найдено логов для загрузки");
        else
            logger.info("Для загрузки найдено {} логов", logsPool.size());
        return logsPool;
    }
}
