package com.clickhouse1ctj.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.yandex.clickhouse.ClickHousePreparedStatementImpl;
import ru.yandex.clickhouse.ClickHouseStatement;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Класс работает с одним экземпляром объекта (синглтон). Объект хранит соответствие между
 * свойствами (полями) и каждым типом события из ТЖ. Информация об это соответствии собирается
 * при загрузке ТЖ и хранится в Clickhouse в таблице {@value TABLENAME}.
 * Данная таблица имеет формат: Колонка event String и Колонка property String, где
 * каждая запись определяется принадлежность указанного свойства (property) к указанному типу события (event)
 */
public class PropertiesByEvents {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesByEvents.class);
    private static final PropertiesByEvents propertiesByEvents = new PropertiesByEvents();

    private static final String TABLENAME = "properties_by_events_tj";
    private final SortedMap<String, Set<String>> mapEventProperties = new TreeMap<>();
    private final List<String[]> newEventsByProperties = new ArrayList<>();

    private PropertiesByEvents() {
        // При создании сразу проверим есть ли таблица
        try {
            if (ClickHouseDDLer.chDDLSync.tableExist(TABLENAME)){
                // Если есть, то прочитаем ее.
                loadPropertiesFromTable();
            } else {
                // Если нет, то создадим новую
                createTable();
            }
        } catch (SQLException e) {
            logger.error("Не удалось проверить наличие таблицы {} : {}", TABLENAME, e.getMessage());
            e.printStackTrace();
        }
    }

    public static void setPropertiesByEvent(String event, Set<String> setFields) {
        for (String field: setFields) {
            propertiesByEvents.setPropertyByEvent(event, field);
        }
    }

    public static void save() throws SQLException {
        propertiesByEvents.insertPropertiesAndEventsInTable();
    }

    private void insertPropertiesAndEventsInTable() throws SQLException {
        if (newEventsByProperties.isEmpty()) return;
        String insertQuery = "INSERT INTO "
                + TABLENAME
                + " (event, property) VALUES (?, ?)";
        synchronized (newEventsByProperties) {
            try (PreparedStatement stmt = ClickHouseDDLer.chDDLSync.getConnection().prepareStatement(insertQuery)) {
                // Добавим все новые записи в таблицу
                for (String[] KeyValue : newEventsByProperties) {
                    stmt.setString(1, KeyValue[0]);
                    stmt.setString(2, KeyValue[1]);
                    stmt.addBatch();
                }
                ((ClickHousePreparedStatementImpl) stmt).executeBatch(ClickHouseDDLer.chDDLSync.chAdditionalDBParams);
                logger.debug("В таблицу {} добавлены новые соответствия событий и свойств: {} записей", TABLENAME, newEventsByProperties.size());
            }
            // Очистим список новых событий и свойств
            newEventsByProperties.clear();
        }
    }

    private void createTable() throws SQLException {
        String query = String.format("CREATE TABLE IF NOT EXISTS %s (%n", TABLENAME) +
                // Добавим колонку для хранения типа события
                "event String, " +
                // Добавим колонку для хранения поля принадлежащего объекту
                "property String" +
                // Укажем движок таблицы
                ") ENGINE = MergeTree " +
                // Сортировка по отметке времени
                "ORDER BY (event) ";
        logger.debug("Создана таблица {}", TABLENAME);
        ClickHouseDDLer.chDDLSync.execQuery(query);
    }

    private void loadPropertiesFromTable() throws SQLException {
        String query = "SELECT event, property FROM " + TABLENAME;
        try (ClickHouseStatement stmt = ClickHouseDDLer.chDDLSync.getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(query)) {
            while (rs.next()) {
                mapEventProperties.computeIfAbsent(
                        rs.getString(1), // Event
                        this::createNewSetPropertiesByEvent) // Создать новое множество
                            .add(rs.getString(2)); // Добавить тип события во множество
            }
        }
    }

    private void setPropertyByEvent(String event, String property) {
        synchronized (newEventsByProperties) {
            Set<String> propertiesByEvent = mapEventProperties.get(event);
            if (propertiesByEvent == null) {
                propertiesByEvent = new HashSet<>(ClickHouseDDLer.getDefaultColumns().keySet());
                logger.trace("Создано множество обязательных свойств события {}: {}", event, propertiesByEvent);
                for (String defaultProperty : propertiesByEvent)
                    newEventsByProperties.add(new String[]{event, defaultProperty});
                mapEventProperties.put(event, propertiesByEvent);
            }

            if (!propertiesByEvent.contains(property)) {
                propertiesByEvent.add(property);
                newEventsByProperties.add(new String[]{event, property}); // Если ранее такой связи не было, то запомним ее
            }
        }
    }

    private Set<String> createNewSetPropertiesByEvent(String event) {
        return new HashSet<>();
    }
}
