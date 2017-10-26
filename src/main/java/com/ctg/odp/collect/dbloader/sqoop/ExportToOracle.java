package com.ctg.odp.collect.dbloader.sqoop;

import java.io.IOException;

import org.apache.sqoop.tool.ExportTool;

import com.cloudera.sqoop.SqoopOptions;
import com.ctg.odp.collect.dbloader.importToOracle.OracleInfo;

public class ExportToOracle {
    private OracleInfo oracleInfo;
    private String hdfsSourceDir;

    public ExportToOracle(OracleInfo oracleInfo) {
        this(oracleInfo, "/apps/odp/data/dbload");
    }

    public ExportToOracle(OracleInfo oracleInfo, String hdfsSourceDir) {
        this.oracleInfo = oracleInfo;
        this.hdfsSourceDir = hdfsSourceDir;
    }

    @SuppressWarnings("deprecation")
    public SqoopOptions getSqoopOptions() throws IOException {
        SqoopOptions options = new SqoopOptions();
        ExportTool exportTool = new ExportTool();
        options.setActiveSqoopTool(exportTool);

        String connecString = "jdbc:oracle:thin:@" + oracleInfo.getOracleHost() + ":1521:" + oracleInfo.getOracleDatabase();
        options.setConnectString("jdbc:oracle:thin:@132.122.1.163:1521:orcl2");
        options.setUsername(oracleInfo.getOracleUserName());
        options.setPassword(oracleInfo.getOraclePassWord());
        options.setDirectMode(true);
        options.setNumMappers(4);
        options.setExportDir(hdfsSourceDir);
        options.setTableName(oracleInfo.getOracleTable().toUpperCase());
        String oracleManager = "org.apache.sqoop.manager.OracleManager";
        options.setConnManagerClassName(oracleManager);
        options.setInputFieldsTerminatedBy('^');
        return options;
    }

    public int exportData() throws IOException {
        @SuppressWarnings("deprecation")
        SqoopOptions options = getSqoopOptions();
        ExportTool exportTool = new ExportTool();
        int result = exportTool.run(options);
        return result;
    }
}
