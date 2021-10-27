package com.clickhouse1ctj.parser;

public class LogRecordParserException extends Exception {
    LogRecordParserException(String message) {super(message);}

    LogRecordParserException(String message, Exception e) {super(message, e);}
}
