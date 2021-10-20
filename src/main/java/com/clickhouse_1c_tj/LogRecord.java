package main.java.com.clickhouse_1c_tj;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

class LogRecord {
    String rawRecord;
    // Key fields:
    LocalDateTime timestamp;
    Long duration;
    String event;
    String level;
    int lineNumberInFile;
    // Other fields:
    private final Map<String, String> logDict = new HashMap<>();

    Set<String> currentLogFields = new HashSet<>();

    private static final DateTimeFormatter timeStampFormat = DateTimeFormatter.ofPattern("yyMMddHHmm:ss.SSSSSS");
    private static final DateTimeFormatter datetimeFormatCH = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public LogRecord(String rawRecord, int lineNumber, String yearMonthDayHour) {
        this.rawRecord = rawRecord;
        this.lineNumberInFile = lineNumber;
        parseRow(yearMonthDayHour);
    }

    public LogRecord(String timestamp, Long duration, String event, String level, int lineNumber) {
        this.rawRecord = "";
        // Ключевые поля записи лога
        this.timestamp = datetimeFormatCH.parse(timestamp, LocalDateTime::from);
        this.duration = duration;
        this.event = event;
        this.level = level;
        this.lineNumberInFile = lineNumber;
    }

    public boolean equals(LogRecord log, boolean checkLineNumber) {
        return timestamp.equals(log.timestamp)
                && duration.equals(log.duration)
                && event.equals(log.event)
                && level.equals(log.level)
                && (!checkLineNumber || lineNumberInFile == log.lineNumberInFile);
    }

    public String get(String key) {
        return logDict.get(key);
    }

    public String getDateTime64CH() {
        return datetimeFormatCH.format(this.timestamp);
    }

    private void parseRow(String yearMonthDayHour) {
        int pos1;
        int pos2;
        int rowLength = rawRecord.length();

        pos1 = rawRecord.indexOf('-');
        String minSecMicrosec = rawRecord.substring(0, pos1);
        timestamp = timeStampFormat.parse(yearMonthDayHour + minSecMicrosec, LocalDateTime::from);
        pos2 = rawRecord.indexOf(',');
        duration = Long.parseLong(rawRecord.substring(pos1 + 1, pos2));
        pos1 = pos2;
        pos2 = rawRecord.indexOf(',', pos1 + 1);
        event = rawRecord.substring(pos1 + 1, pos2);
        pos1 = pos2;
        pos2 = rawRecord.indexOf(',', pos1 + 1);
        level = rawRecord.substring(pos1 + 1, pos2);
        while (pos2 != -1 && pos2 < rowLength) {
            String key;
            String value;

            pos1 = pos2;
            pos2 = rawRecord.indexOf('=', pos1 + 1);
            if (pos2 == -1 || pos2 + 1 >= rowLength)
                // Битая строка или строка которая заканчивается без значения ключа. Например, ",ConnectionString="
                return;
            else
                key = rawRecord.substring(pos1 + 1, pos2).replace(":", "");

            pos1 = pos2 + 1;
            char curCh = rawRecord.charAt(pos1);
            if (curCh == '\'' || curCh == '"') {
                // Значение параметра содержит строку в кавычках
                pos2 = rawRecord.indexOf(curCh, pos1 + 1);
                // Проверим, что это не экранированные кавычки
                while (pos2 + 1 != rowLength && rawRecord.charAt(pos2 + 1) != ',') {
                    pos2 = rawRecord.indexOf(curCh, pos2 + 1);
                }
                value = rawRecord.substring(pos1 + 1, pos2);
                pos2++;
            } else {
                pos2 = rawRecord.indexOf(',', pos1);
                if (pos2 == -1)
                    value = rawRecord.substring(pos1);
                else
                    value = rawRecord.substring(pos1, pos2);
            }
            logDict.put(key, value);
            currentLogFields.add(key);
        }
    }

    @Override
    public String toString() {
        return "ЗаписьТЖ{" +
                "[" + lineNumberInFile +
                "]:timestamp=" + timestamp +
                ", duration=" + duration +
                ", event='" + event + '\'' +
                ", level='" + level + '\'' +
                '}';
    }
}
