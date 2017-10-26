package com.ctg.odp.collect.dbloader.importToOracle;

public class MysqlInfo {
    // 连接mysql的相关信息
    private String mysqlHost;
    private String mysqlDatabase;
    private String mysqlUserName;
    private String mysqlPassWord;
    private String mysqlTable;

    public MysqlInfo(String mysqlHost, String mysqlDatabase, String mysqlUserName, String mysqlPassWord, String mysqlTable) {
        this.mysqlHost = mysqlHost;
        this.mysqlDatabase = mysqlDatabase;
        this.mysqlUserName = mysqlUserName;
        this.mysqlPassWord = mysqlPassWord;
        this.mysqlTable = mysqlTable;
    }

    public String getMysqlHost() {
        return mysqlHost;
    }

    public void setMysqlHost(String mysqlHost) {
        this.mysqlHost = mysqlHost;
    }

    public String getMysqlDatabase() {
        return mysqlDatabase;
    }

    public void setMysqlDatabase(String mysqlDatabase) {
        this.mysqlDatabase = mysqlDatabase;
    }

    public String getMysqlUserName() {
        return mysqlUserName;
    }

    public void setMysqlUserName(String mysqlUserName) {
        this.mysqlUserName = mysqlUserName;
    }

    public String getMysqlPassWord() {
        return mysqlPassWord;
    }

    public void setMysqlPassWord(String mysqlPassWord) {
        this.mysqlPassWord = mysqlPassWord;
    }

    public String getMysqlTable() {
        return mysqlTable;
    }

    public void setMysqlTable(String mysqlTable) {
        this.mysqlTable = mysqlTable;
    }

    @Override
    public String toString() {
        return "MysqlInfo [mysqlHost=" + mysqlHost + ", mysqlDatabase=" + mysqlDatabase + ", mysqlUserName=" + mysqlUserName + ", mysqlPassWord="
                + mysqlPassWord + ", mysqlTable=" + mysqlTable + "]";
    }

}
