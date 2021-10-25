package com.clickhouse1ctj;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

class LogRecordParser {
    static final Logger logger = LoggerFactory.getLogger(LogRecordParser.class);
    String rawRecord;
    private final int rowLength;
    private final Map<String, String> logDict = new HashMap<>();
    private int pos1 = 0;
    private int pos2 = 0;
    private static final char FIELD_SPLITTER = ',';
    private static final char KEY_VALUE_SPLITTER = '=';
    private static final Pattern minSecMicrosecPattern = Pattern.compile("^\\d\\d:\\d\\d\\.\\d{6}");

    LogRecordParser(String rawRecord) throws LogRecordParserException {
        this.rawRecord = rawRecord;
        this.rowLength = rawRecord.length();
        parseRecord();
    }

    public Map<String, String> getDict() {
        return logDict;
    }

    private void parseRecord() throws LogRecordParserException {
        parseFixPart();
        parseVariablePart();
    }

    private void parseFixPart() throws LogRecordParserException {
        // 01:32.736430- - начало записи события
        pos1 = rawRecord.indexOf('-');
        String minSecMicrosec = rawRecord.substring(0, pos1);
        logDict.put("minSecMicrosec", minSecMicrosec);
        if (!minSecMicrosecPattern.matcher(minSecMicrosec).find()) {
            throw new LogRecordParserException(
                    String.format("Начало записи лога не соответствует шаблону \"%s\": %s",
                            minSecMicrosecPattern,
                            rawRecord));
        }
        // ToDo предусмотреть выброс исключения, для "битой" записи, например, если не все ключевые значения присутствуют
        // -837213, - длительность события
        pos2 = rawRecord.indexOf(FIELD_SPLITTER);
        logDict.put("duration", rawRecord.substring(pos1 + 1, pos2));
        // ,CALL, - вид события
        pos1 = pos2;
        pos2 = rawRecord.indexOf(FIELD_SPLITTER, pos1 + 1);
        logDict.put("event", rawRecord.substring(pos1 + 1, pos2));
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
                logger.debug("Дубль свойства {} в записи лога текущее значение: {}, дополнительное: {} ", key, currentValue, value);
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
        } else if (pos2 + 1 >= rawRecord.length()) {
            // Строка заканчивается без значения ключа. Например, ",ConnectionString="
            // ToDo - имеет смысл все таки возвращать key, а проверку на продолжение парсинга строки выполнять уровнем выше
            return null;
        } else {
            String key = rawRecord.substring(pos1 + 1, pos2).replace(":", "");
            pos1 = pos2 + 1;
            return key;
        }
    }

    private String getValueInParsingRow() {
        String value;
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
