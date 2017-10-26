package com.ctg.odp.collect.dbloader.sqoop;

import java.io.IOException;

import org.apache.sqoop.tool.ImportAllTablesTool;
import org.apache.sqoop.tool.ImportTool;

import com.cloudera.sqoop.SqoopOptions;
import com.ctg.odp.collect.dbloader.importToOracle.MysqlInfo;

public class ImportToHDFS {
    private MysqlInfo mysqlInfo;
    private String hdfsTargetDir;
    private String SQLString;

    public ImportToHDFS(MysqlInfo mysqlInfo) {
        // this(mysqlInfo, "/apps/odp/data/dbloadTest");
    }


    public ImportToHDFS(MysqlInfo mysqlInfo, String hdfsTargetDir, String SQLString) {
        this.mysqlInfo = mysqlInfo;
        this.hdfsTargetDir = hdfsTargetDir;
        this.SQLString = SQLString;
    }

    @SuppressWarnings("deprecation")
    public SqoopOptions getSqoopOptions() throws IOException {
        String targetDir = hdfsTargetDir;
        SqoopOptions options = new SqoopOptions();
        ImportTool importTool = new ImportTool();
        options.setActiveSqoopTool(importTool);
        String connecString = "jdbc:mysql://" + mysqlInfo.getMysqlHost() + ":3306/" + mysqlInfo.getMysqlDatabase();
        options.setConnectString(connecString);
        options.setUsername(mysqlInfo.getMysqlUserName());
        options.setPassword(mysqlInfo.getMysqlPassWord());
        // options.setTableName(mysqlInfo.getMysqlTable());
        options.setSqlQuery(SQLString);
        options.setTargetDir(targetDir);
        options.setNumMappers(4);
        options.setDriverClassName("com.mysql.jdbc.Driver");
        options.setNullNonStringValue("");
        // options.setDeleteMode(true);
        options.setVerbose(true);
        options.setSplitByCol("1");
        options.setFieldsTerminatedBy('^');
        return options;
    }

    // 导入指定表的数据
    public int importData() throws IOException {
        @SuppressWarnings("deprecation")
        SqoopOptions options = getSqoopOptions();
        ImportTool importTablesTool = new ImportTool();
        int result = importTablesTool.run(options);
        return result;
    }

    // 导入所有表的数据
    public int importAllTableData() throws IOException {
        @SuppressWarnings("deprecation")
        SqoopOptions options = getSqoopOptions();
        ImportAllTablesTool importAllTablesTool = new ImportAllTablesTool();
        int result = importAllTablesTool.run(options);
        return result;
    }
}
