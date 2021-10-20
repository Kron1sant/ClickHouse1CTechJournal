package main.java.com.clickhouse1ctj.config;

public class ClickHouseConnect {
    private String host;
    private String port;
    private String user;
    private String pass;
    private String database;
    private String tablePostfix;
    private String engine;
    private String orderBy;
    private String partition;

    ClickHouseConnect() {
        setHost("localhost");
        setPort("8123");
        setUser("default");
        setPass("");
        setDatabase("default");
        setTablePostfix("Main");
        setEngine("MergeTree");
        setOrderBy("datetime, event");
        setPartition("toHour(datetime), source");
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getTablePostfix() {
        return tablePostfix;
    }

    public void setTablePostfix(String tablePostfix) {
        this.tablePostfix = tablePostfix;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    @Override
    public String toString() {
        return "ClickHouseConnect{" +
                "host='" + host + '\'' +
                ", port='" + port + '\'' +
                ", user='" + user + '\'' +
                ", pass='" + pass + '\'' +
                ", database='" + database + '\'' +
                ", engine='" + engine + '\'' +
                '}';
    }
}
