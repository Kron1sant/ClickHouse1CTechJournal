package com.clickhouse1ctj.loader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

class TableLock {
    private static final Logger logger = LoggerFactory.getLogger(TableLock.class);
    private static final Map<String, TableLock> tableLocks = new HashMap<>();
    private int semaphore;

    public TableLock(String tablename) {
        logger.trace("Создан объект блокировки для таблицы {}", tablename);
    }

    public static synchronized TableLock getTableLock(String tablename) {
        return tableLocks.computeIfAbsent(tablename, TableLock::new);
    }

    public void check() {
        logger.debug("Начало проверки семафора. Текущее значение {}", semaphore);
        while (semaphore != 0) {
            logger.trace("Повторная попытка проверки семафора. Текущее значение {}", semaphore);
            try {
                //noinspection BusyWait
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
        logger.debug("Семафор доступен. Текущее значение {}", semaphore);
    }

    public void down() {
        synchronized (this) {
            // Погружение с монитором. Не сможем продолжить, пока удерживается объект в другом потоке
            semaphore++;
            logger.debug("Захват семафора. Текущее значение {}", semaphore);
        }
    }

    public void up() {
        // Возврат из критической области без монитора. Сможем всегда вернуться, даже если объект удерживается
        semaphore--;
        logger.debug("Возврат семафора. Текущее значение {}", semaphore);
    }
}
