package main.java.com.clickhouse1ctj;

public class LogRecordParserException extends Exception {
    LogRecordParserException(String message) {super(message);}

    LogRecordParserException(String message, Exception e) {super(message, e);}
}
