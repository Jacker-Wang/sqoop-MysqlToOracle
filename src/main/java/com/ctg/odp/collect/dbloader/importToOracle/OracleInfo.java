package com.ctg.odp.collect.dbloader.importToOracle;

public class OracleInfo {
    // 连接oracle的相关信息
    private String oracleHost;
    private String oracleDatabase;
    private String oracleUserName;
    private String oraclePassWord;
    private String oracleTable;

    public OracleInfo(String oracleHost, String oracleDatabase, String oracleUserName, String oraclePassWord, String oracleTable) {
        super();
        this.oracleHost = oracleHost;
        this.oracleDatabase = oracleDatabase;
        this.oracleUserName = oracleUserName;
        this.oraclePassWord = oraclePassWord;
        this.oracleTable = oracleTable;
    }

    public String getOracleHost() {
        return oracleHost;
    }

    public void setOracleHost(String oracleHost) {
        this.oracleHost = oracleHost;
    }

    public String getOracleDatabase() {
        return oracleDatabase;
    }

    public void setOracleDatabase(String oracleDatabase) {
        this.oracleDatabase = oracleDatabase;
    }

    public String getOracleUserName() {
        return oracleUserName;
    }

    public void setOracleUserName(String oracleUserName) {
        this.oracleUserName = oracleUserName;
    }

    public String getOraclePassWord() {
        return oraclePassWord;
    }

    public void setOraclePassWord(String oraclePassWord) {
        this.oraclePassWord = oraclePassWord;
    }

    public String getOracleTable() {
        return oracleTable;
    }

    public void setOracleTable(String oracleTable) {
        this.oracleTable = oracleTable;
    }

    @Override
    public String toString() {
        return "OracleInfo [oracleHost=" + oracleHost + ", oracleDatabase=" + oracleDatabase + ", oracleUserName=" + oracleUserName + ", oraclePassWord="
                + oraclePassWord + ", oracleTable=" + oracleTable + "]";
    }

}
