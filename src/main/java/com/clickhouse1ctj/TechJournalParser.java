package com.clickhouse1ctj;

import java.io.*;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TechJournalParser {
    static final Logger logger = LoggerFactory.getLogger(TechJournalParser.class);

    Path pathToLog;
    String filename;
    String yearMonthDayHour;
    String parentName;
    String source;
    int parentPid;
    BufferedReader logFile;
    Long size;
    int recordCount;
    private static final Pattern startLogPattern = Pattern.compile("^\\d\\d:\\d\\d\\.\\d+-\\d+,");
    private String previousLine;
    private int currentLineNumber;
    private int previousLineNumber;
    Set<String> logFields = new HashSet<>();
    private boolean completed = false;


    public TechJournalParser(Path pathToLog) throws FileNotFoundException {
        this.pathToLog = pathToLog;
        filename = pathToLog.getFileName().toString();
        if (filename.length() != 12 || !isDigit(filename.substring(0, 8))) {
            logger.error("Некорректное имя файла {}. Должен быть формат YYMMDDHH.log", filename);
            return;
        }
        yearMonthDayHour = filename.substring(0, 8);
        parentName = pathToLog.getParent().getFileName().toString();
        parentPid = getPID(parentName);
        source = getSource(parentName);
        logFile = new BufferedReader(
                new FileReader(pathToLog.toAbsolutePath().toString()));
        size = (new File(pathToLog.toAbsolutePath().toString())).length();
        recordCount = 0;
        currentLineNumber = 0;
        logger.info("Файл {} размером {} байт готов для парсинга", this.pathToLog.toAbsolutePath(), size);
    }

    public boolean isEmpty() {
        return size <= 3;
    }

    public boolean isCompleted() {
        return completed;
    }

    public List<LogRecord> getNextRecords(int count, LogRecord lastRecord) {
        if (previousLine == null) {
            // Только начали чтение, первая строка может начинаться с BOM-символов
            previousLine = eraseBOM(readNextLine());
            previousLineNumber = 1;
        }

        // Если указана последняя запись, от которой нужно парсить лог, то пропускаем строки до нее (включая ее)
        boolean skipRecords = lastRecord != null;

        List<LogRecord> batch = new ArrayList<>(); // Итоговый пакет записей лога
        int i = 0;
        while (i < count) {
            StringBuilder sb = new StringBuilder();
            sb.append(previousLine);
            // Собираем одну запись лога поочередно читая строки файла,
            // пока не встретим очередную строку, которая начинается с шаблона startLogPattern
            String currentLine = readNextLine();
            while (currentLine != null
                    && !startLogPattern.matcher(currentLine).find()) {
                sb.append(currentLine);
                currentLine = readNextLine();
            }
            try {
                LogRecord logRecord = new LogRecord(sb.toString(), previousLineNumber, yearMonthDayHour);
                if (!skipRecords) {
                    // Добавляем запись в пакет
                    batch.add(logRecord);
                    // Отдельно запоминаем все поля из лога - эта информация нужна для обновления колонок в таблице ClickHouse
                    logFields.addAll(logRecord.currentLogFields);
                    recordCount++;
                    i++;
                } else if (logRecord.equals(lastRecord, false)) {
                    // Если текущая запись лога равна последней записи из базы, то снимаем метку slipRecords
                    // и на следующей итерации начинаем формировать пакет к загрузке
                    skipRecords = false;
                }
            } catch (LogRecordParserException e) {
                logger.info("Не удалось распарсить строку лога: {}. По причине {}", sb, e.getMessage());
            }

            if (currentLine == null) {
                // Конец файла наступил раньше, чем выбрали весь пакет
                parsingCompleted();
                break;
            }
            // Текущая прочитанная строка является началом новой записи лога,
            // так как соответствует шаблону startLogPattern. Запомним ее как previousLine для следующей итерации
            previousLine = currentLine;
            previousLineNumber = currentLineNumber;
        }
        return batch;
    }

    private String readNextLine() {
        String line = "";
        try {
            line = logFile.readLine();
            currentLineNumber++;
        } catch (IOException e) {
            logger.error("Не удалось прочитать строку файла {}", pathToLog.toAbsolutePath());
            e.printStackTrace();
        }
        return line;
    }

    private String eraseBOM(String line) {
        int shift = 0;
        while (!Character.isDigit(line.charAt(shift))
                && shift<line.length() - 1) {
            shift++;
        }
        return line.substring(shift); // Сдвиг на количество BOM-символов
    }

    private void parsingCompleted() {
        completed = true;
        try {
            logFile.close();
        } catch (IOException e) {
            logger.error("Не удалось закрыть файл после парсинга {}", pathToLog.toAbsolutePath());
            e.printStackTrace();
        }
        logger.info("Завершен парсинг. Всего обработано {} записей из файла {} ", recordCount, pathToLog.toAbsolutePath());
    }

    private int getPID(String parentName) {
        String[] nameParts = parentName.split("_");
        if (nameParts.length == 2 && isDigit(nameParts[1])) {
            return Integer.parseInt(nameParts[1]);
        } else {
            logger.error("Не удалось получить PID из каталога файла ТЖ {}", pathToLog.toAbsolutePath());
            return 0;
        }
    }

    private String getSource(String parentName) {
        String[] nameParts = parentName.split("_");
        if (nameParts.length == 2) {
            return nameParts[0];
        } else {
            logger.error("Не удалось получить тип процесса источника ТЖ по имени каталога логов {}", pathToLog.toAbsolutePath());
            return "None";
        }
    }

    private boolean isDigit(String str) {
        return str.chars().allMatch(Character::isDigit);
    }
}
