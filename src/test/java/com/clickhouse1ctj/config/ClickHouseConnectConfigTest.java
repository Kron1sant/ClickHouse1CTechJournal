package com.clickhouse1ctj.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

class ClickHouseConnectConfigTest {
    static final ClickHouseConnectConfig testCHConfigGetter = AppConfig.getConfig(AppConfigTest.TEST_CONFIG_PATH).clickhouse;
    static final ClickHouseConnectConfig testCHConfigSetter = AppConfig.getConfig(AppConfigTest.TEST_CONFIG_PATH).clickhouse;

    @Nested
    class ClickHouseConnectConfigTestGetter {
        @Test
        void getHost() {
            assertEquals("test_host", testCHConfigGetter.getHost());
        }

        @Test
        void getPort() {
            assertEquals("9876", testCHConfigGetter.getPort());
        }

       @Test
        void getUser() {
            assertEquals("test_user", testCHConfigGetter.getUser());
        }

        @Test
        void getPass() {
            assertEquals("test_pass", testCHConfigGetter.getPass());
        }

        @Test
        void getDatabase() {
            assertEquals("test_base", testCHConfigGetter.getDatabase());
        }

        @Test
        void getTablePostfix() {
            assertEquals("test_postfix", testCHConfigGetter.getTablePostfix());
        }

        @Test
        void getEngine() {
            assertEquals("test_engine", testCHConfigGetter.getEngine());
        }

        @Test
        void getOrderBy() {
            assertEquals("test_order1, test_order2", testCHConfigGetter.getOrderBy());
        }

        @Test
        void getPartition() {
            assertEquals("test_pert1, test_part2", testCHConfigGetter.getPartition());
        }

    }

    @Nested
    class ClickHouseConnectConfigTestSetter {
        @Test
        void setHost() {
            testCHConfigSetter.setHost("test_host2");
            assertEquals("test_host2", testCHConfigSetter.getHost());
        }

        @Test
        void setPort() {
            testCHConfigSetter.setPort("8123");
            assertEquals("8123", testCHConfigSetter.getPort());
        }

        @Test
        void setUser() {
            testCHConfigSetter.setUser("test_user2");
            assertEquals("test_user2", testCHConfigSetter.getUser());
        }

        @Test
        void setPass() {
            testCHConfigSetter.setPass("test_pass2");
            assertEquals("test_pass2", testCHConfigSetter.getPass());
        }

        @Test
        void setDatabase() {
            testCHConfigSetter.setDatabase("test_base2");
            assertEquals("test_base2", testCHConfigSetter.getDatabase());
        }

        @Test
        void setTablePostfix() {
            testCHConfigSetter.setTablePostfix("test_postfix2");
            assertEquals("test_postfix2", testCHConfigSetter.getTablePostfix());
        }

        @Test
        void setEngine() {
            testCHConfigSetter.setEngine("test_engine2");
            assertEquals("test_engine2", testCHConfigSetter.getEngine());
        }

        @Test
        void setOrderBy() {
            testCHConfigSetter.setOrderBy("test_order3, test_order4");
            assertEquals("test_order3, test_order4", testCHConfigSetter.getOrderBy());
        }

        @Test
        void setPartition() {
            testCHConfigSetter.setPartition("test_pert3, test_part4");
            assertEquals("test_pert3, test_part4", testCHConfigSetter.getPartition());
        }
    }
}