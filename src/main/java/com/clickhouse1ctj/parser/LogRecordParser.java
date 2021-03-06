package com.clickhouse1ctj.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

class LogRecordParser {
    static final Logger logger = LoggerFactory.getLogger(LogRecordParser.class);
    static final ConcurrentMap<String, String> cacheNormalizedProperties = new ConcurrentHashMap<>();
    static final Pattern propertyKeyFormat = Pattern.compile("^[a-zA-Z_][0-9a-zA-Z_]*$");
    private final String rawRecord;
    private final int rowLength;
    private final SortedMap<String, String> logDict = new TreeMap<>();
    private int pos1 = 0;
    private int pos2 = 0;
    private static final char FIELD_SPLITTER = ',';
    private static final char KEY_VALUE_SPLITTER = '=';
    private static final Pattern mandatoryLogPartPattern = Pattern.compile("^\\d\\d:\\d\\d\\.\\d{6}-\\d+,[a-zA-Z]+,\\d+,");

    LogRecordParser(String rawRecord) throws LogRecordParserException {
        this.rawRecord = rawRecord;
        this.rowLength = rawRecord.length();
        parseRecord();
    }

    public SortedMap<String, String> getDict() {
        return logDict;
    }

    private void parseRecord() throws LogRecordParserException {
        parseFixPart();
        parseVariablePart();
    }

    private void parseFixPart() throws LogRecordParserException {
        // Проверка обязательной части в начале лога, например: 01:32.736453-23212,CALL,3,
        if (!mandatoryLogPartPattern.matcher(rawRecord).find()) {
            throw new LogRecordParserException(
                    String.format("Начало записи лога не соответствует шаблону \"%s\"",
                            mandatoryLogPartPattern));
        }
        // 01:32.736430- - начало записи события
        pos1 = rawRecord.indexOf('-');
        String minSecMicrosec = rawRecord.substring(0, pos1);
        logDict.put("minSecMicrosec", minSecMicrosec);

        // -837213, - длительность события
        pos2 = rawRecord.indexOf(FIELD_SPLITTER);
        logDict.put("duration", rawRecord.substring(pos1 + 1, pos2));
        // ,CALL, - вид события
        pos1 = pos2;
        pos2 = rawRecord.indexOf(FIELD_SPLITTER, pos1 + 1);
        logDict.put("event", rawRecord.substring(pos1 + 1, pos2).toUpperCase());
        // ,2, - уровень события
        pos1 = pos2;
        pos2 = rawRecord.indexOf(FIELD_SPLITTER, pos1 + 1);
        logDict.put("level", rawRecord.substring(pos1 + 1, pos2));
    }

    private void parseVariablePart() throws LogRecordParserException {
        pos1 = pos2;
        // Пока позиция начала ключа определена и не превышает длину файла ищем пару "Ключ-Значение"
        while (pos1 != -1 && pos1 < rowLength) {
            String key = getKeyInParsingRow();
            if (key == null) {
                // Завершим парсинг строки
                break;
            }
            String value;
            try {
                value = getValueInParsingRow();
            } catch (StringIndexOutOfBoundsException e) {
                throw new LogRecordParserException(String.format("Ошибка в парсинге значения ключа: %s", e.getMessage()), e);
            }

            // К сожалению, ключи свойств в записи лога могут совпадать. Например, p:processName
            // В этом случае запишем все значения через запятую.
            String currentValue = logDict.get(key);
            if (currentValue == null)
                logDict.put(key, value);
            else {
                logger.trace("Дубль свойства {} в записи лога текущее значение: {}, дополнительное: {} ", key, currentValue, value);
                logDict.put(key, currentValue + "," + value);
            }
            pos1 = pos2;
        }
    }

    private String getKeyInParsingRow() {
        // Например: ,OSThread= - ключ в очередном свойстве лога
        pos2 = rawRecord.indexOf(KEY_VALUE_SPLITTER, pos1 + 1);
        if (pos2 == -1) {
            // Битая строка
            logger.info("Неожиданный конец строки лога {}", rawRecord);
            return null;
        }

        // Чтобы каждый раз не нормализовывать имена свойств, будем их кэшировать.
        String key = cacheNormalizedProperties.computeIfAbsent(
                rawRecord.substring(pos1 + 1, pos2), this::normalizePropertyKey);
        pos1 = pos2 + 1;
        return key;
    }

    /**
     * Некоторые свойства могут иметь следующий формат: ProcessList[pid, mem(Kb)] или p:processName
     * Такие поля не соответствуют требованиям к идентификаторам в ClickHouse - ^[a-zA-Z_][0-9a-zA-Z_]*$
     * Возможны варианты: удалить недопустимые символы, или использовать кавычки
     * (ClickHouse допускает двойные или обратные).
     */
    private String normalizePropertyKey(String key) {
        // Используем кавычки:
        if (!propertyKeyFormat.matcher(key).find())
            return "\"" + key + "\"";
        else
            return key;
    }

    private String getValueInParsingRow() {
        String value;
        // Строка заканчивается пустым значением. Например, ",ConnectionString="
        // В этом случае позиция текущего символа выходит за границу строки,
        // проверим и, если это так, то вернем пустую строку
        if (pos1 == rawRecord.length()) {
            pos2 = pos1; // важно сдвинуть и pos2 за границы строки, чтобы на следующей итерации завершить циклы
            return "";
        }
        // Символ из текущей позиции строки
        char curCh = rawRecord.charAt(pos1);
        if (curCh == '\'' || curCh == '"') {
            // Значение параметра содержит строку в кавычках (одинарных или двойных)
            pos2 = rawRecord.indexOf(curCh, pos1 + 1);
            // Проверим, что это не экранированные кавычки
            while (pos2 + 1 != rowLength && rawRecord.charAt(pos2 + 1) != FIELD_SPLITTER) {
                // После кавычки не идет запятая и это не конец строки,
                // следовательно, ищем закрывающую кавычка дальше.
                // Смещаемся на 2 символа вправо (!!!), так как при смещении на один символ
                // мы попадаем на экранированную кавычку
                pos2 = rawRecord.indexOf(curCh, pos2 + 2);
                if (pos2 == -1) {
                    // Парная кавычка не найдена - строка обрывается.
                    // Можно признать, что значение битое, вернув null или кинув исключение.
                    // Или вернуть имеющуюся часть до конца строки.
                    // Пойдем вторым вариантом:
                    pos2 = rowLength;
                    break;
                }
            }
            value = rawRecord.substring(pos1 + 1, pos2);
            pos2++;
        } else {
            pos2 = rawRecord.indexOf(FIELD_SPLITTER, pos1);
            if (pos2 == -1)
                value = rawRecord.substring(pos1);
            else
                value = rawRecord.substring(pos1, pos2);
        }
        return value;
    }
}
