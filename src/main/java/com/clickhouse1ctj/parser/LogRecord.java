package com.clickhouse1ctj.parser;

import com.clickhouse1ctj.loader.PropertiesByEvents;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class LogRecord {
    // Key fields:
    private static final List<String> KEY_FIELDS = List.of("timestamp", "duration", "event", "level", "lineNumberInFile");
    private final LocalDateTime timestamp;
    private final Long duration;
    private final String event;
    private final String level;
    private final int lineNumberInFile;
    // Other fields:
    private final Map<String, String> logDict = new HashMap<>();
    public final Set<String> currentLogFields = new HashSet<>(KEY_FIELDS); // все поля текущей записи лога

    private static final DateTimeFormatter timeStampFormat = DateTimeFormatter.ofPattern("yyMMddHHmm:ss.SSSSSS");
    private static final DateTimeFormatter datetimeFormatCH = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public LogRecord(String rawRecord, int lineNumber, String yearMonthDayHour) throws LogRecordParserException {
        LogRecordParser parser = new LogRecordParser(rawRecord);
        SortedMap<String, String> fullParsedDict = parser.getDict();
        this.lineNumberInFile = lineNumber;
        // Fixed fields
        this.timestamp = timeStampFormat.parse(yearMonthDayHour + fullParsedDict.get("minSecMicrosec"), LocalDateTime::from);
        this.duration = Long.parseLong(fullParsedDict.get("duration"));
        this.event = fullParsedDict.get("event");
        this.level = fullParsedDict.get("level");
        // Misc fields
        for (Map.Entry<String, String> entry: fullParsedDict.entrySet()) {
            String key = entry.getKey();
            if (key.equals("minSecMicrosec") || key.equals("duration") || key.equals("event") || key.equals("level"))
                continue;
            this.logDict.put(key, entry.getValue());
        }
        this.currentLogFields.addAll(this.logDict.keySet()); // Запомним поля текущего лога
        PropertiesByEvents.setPropertiesByEvent(this.event, this.logDict.keySet()); // Отдельно сохраним соответствие: событие -> необязательные свойства
    }

    public LogRecord(String timestampStr, Long duration, String event, String level, int lineNumber) {
        // Ключевые поля записи лога:
        // Строка timestampStr должна иметь формат yyyy-MM-dd HH:mm:ss.SSSSSS
        this.timestamp = datetimeFormatCH.parse(timestampStr, LocalDateTime::from);
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

    public Long getDuration() {
        return duration;
    }

    public String getEvent() {
        return event;
    }

    public String getLevel() {
        return level;
    }

    public int getLineNumberInFile() {
        return lineNumberInFile;
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

