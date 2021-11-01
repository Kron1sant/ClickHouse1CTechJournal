package com.clickhouse1ctj.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ConfigKeysTest {

    @Test
    void getKey() {
        ConfigKeys k = ConfigKeys.valueOf("CH_HOST");
        assertEquals("CH_HOST", k.getKey());
    }

    @Test
    void getSetterName() {
        ConfigKeys k = ConfigKeys.valueOf("CH_HOST");
        assertEquals("clickhouse.setHost", k.getSetterName());
    }

    @Test
    void getValueType() {
        ConfigKeys k = ConfigKeys.valueOf("CH_HOST");
        assertEquals(String.class, k.getValueType());
    }
}