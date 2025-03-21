package cn.berry.rapids.configuration;

public class ClickHouseConfig {

    private String host;

    private String database;

    private String user;

    private String password;

    private Long queryTimeout;

    private Long connectionTimeout;

    private Integer connectTimeout;

    private Integer connectionMaxIdle;

    private Integer connectionMinIdle;

    private Integer connectionTotal;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Long getQueryTimeout() {
        return queryTimeout;
    }

    public void setQueryTimeout(Long queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    public Long getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(Long connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public Integer getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Integer connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public Integer getConnectionMaxIdle() {
        return connectionMaxIdle;
    }

    public void setConnectionMaxIdle(Integer connectionMaxIdle) {
        this.connectionMaxIdle = connectionMaxIdle;
    }

    public Integer getConnectionMinIdle() {
        return connectionMinIdle;
    }

    public void setConnectionMinIdle(Integer connectionMinIdle) {
        this.connectionMinIdle = connectionMinIdle;
    }

    public Integer getConnectionTotal() {
        return connectionTotal;
    }

    public void setConnectionTotal(Integer connectionTotal) {
        this.connectionTotal = connectionTotal;
    }
}
