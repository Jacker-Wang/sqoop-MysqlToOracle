---
layout: "post"
title: "基于Hadoop（M/R）的MySQL到Oracle海量数据切割"
date: "2017-10-17 22:48"
---

# 背景介绍
---
大数据时代，海量数据的迁移会很普遍地出现在各个应用场景，本文主要讨论利用Sqoop的分布式能力从关系型数据库MySQL到Oracle的海量数据迁移和切割。

# 所需环境
---
1 JDK+Eclipse；

2 Hadoop环境（version-2.6.5）

3 Sqoop1.4.6-alpher（sqoop-1.4.6.bin__hadoop-2.0.4-alpha）

# 实现细节
---

## 代码说明
这里只是大致介绍数据迁移实现的流程，具体代码可在[GitHub]下载
  [GitHub]: https://github.com/Jacker-Wang/sqoop-MysqlToOracle "GitHub"

## Java实现所需maven依赖
所需要的maven依赖包主要有：

1 sqoop1.4.6版本的包（sqoop目前有版本1和版本2。sqoop1.4.6对应sqoop1，sqoop1.99.7对应于sqoop2。maven中的sqoop依赖下载不了，所以需要将sqoop-1.4.6.bin__hadoop-2.0.4-alpha中的sqoop-1.4.6.jar拷贝到你的本地仓库对应的位置）。

2 连接MySQL的jar包mysql-connector-java。

3 连接Oracle的jar包oracle-ojdbc7。

4 hadoop基础包

5 MapReduce基础包

```
<properties>
		<project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
		<hadoop.version>2.5.0</hadoop.version>
	</properties>

	<dependencies>

		<dependency>
			<groupId>org.apache.sqoop</groupId>
			<artifactId>sqoop</artifactId>
			<version>1.4.6</version>
		</dependency>
		<dependency>
			<groupId>org.apache.commons</groupId>
			<artifactId>commons-lang3</artifactId>
			<version>3.3.2</version>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-hdfs</artifactId>
			<version>${hadoop.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-common</artifactId>
			<version>${hadoop.version}</version>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-mapreduce-client-core</artifactId>
			<version>${hadoop.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-mapreduce-client-app</artifactId>
			<version>${hadoop.version}</version>
		</dependency>

		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-mapreduce-client-common</artifactId>
			<version>${hadoop.version}</version>
		</dependency>

		<dependency>
			<groupId>mysql</groupId>
			<artifactId>mysql-connector-java</artifactId>
			<version>5.1.36</version>
		</dependency>
		<dependency>
			<groupId>com.oracle</groupId>
			<artifactId>oracle-ojdbc7</artifactId>
			<version>12.1.0.2</version>
		</dependency>
	</dependencies>
```
## 我的主类
一 主类说明：
主类主要的实现步骤如下

1：读取配置文件中的Mysql，Oracle数据连接信息，HDFS目标目录，映射表名称，和映射表相关的序列名称。

2：实现静态MySQL，Oracle操作类，用以建立相关表，获取外键关联表等相关数据。

3：建立静态映射表（映射表作用是映射MySQL表名称到Oracle表名称，方便后续的使用）。

4：数据导入函数的实现importMySQLToOracle的实现，函数是递归的，因为导入数据之前，由于约束的原因，需要先导入外键关联表的数据，所以这里需要递归建立外键关联表和导入外间关联表的数据。

5：最后将数据导入的记录添加到映射表中。

二 代码实现：
```
package com.ctg.odp.collect.dbloader.importToOracle;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ctg.odp.collect.dbloader.sqoop.ExportToOracle;
import com.ctg.odp.collect.dbloader.sqoop.ImportToHDFS;

public class ImportData {
    private static String mapTable = null;
    private static String sequence = null;
    private static CommandLine commandLine = null;
    private static String targetDir = null;
    private static final Log LOG = LogFactory.getLog(ImportData.class);
    private static MysqlInfo mysqlInfo = null;
    private static OracleInfo oracleInfo = null;

    private static OperateMysql operateMysql = null;
    private static OperateOracle operateOracle = null;

    static {
        mapTable = DataUtil.getField("MapTableName");
        sequence = DataUtil.getField("SequenceName");
        targetDir = DataUtil.getField("targetDir");
        LOG.info("****读取到的targetDir连接信息****\n" + targetDir);
    }

    public static void main(String[] args) {
        commandLine = DataUtil.getCommandLine(args);

        mysqlInfo = DataUtil.getMysqlInfo(commandLine);
        LOG.info("****读取到的MySQL连接信息****\n" + mysqlInfo);
        oracleInfo = DataUtil.getOracleInfo(commandLine);
        LOG.info("****读取到的Oracle连接信息****\n" + oracleInfo);

        operateMysql = new OperateMysql(mysqlInfo);
        operateOracle = new OperateOracle(oracleInfo, mapTable, sequence);

        // 建立索引表
        operateOracle.createMapTable();

        // 没有指定表名，则导出所有表的数据
        if (mysqlInfo.getMysqlTable() == null) {
            List<String> tablesList = operateMysql.getAllTables();
            for (String table : tablesList) {
                LOG.info("****开始导入数据表****\n" + table);
                mysqlInfo.setMysqlTable(table);
                oracleInfo.setOracleTable(table);
                operateOracle.setOracleInfo(oracleInfo);
                operateMysql.setMysqlInfo(mysqlInfo);
                importMySQLToOracle(operateMysql, operateOracle, targetDir);
            }
        } else {
            LOG.info("****开始导入数据表****\n" + mysqlInfo.getMysqlTable());
            importMySQLToOracle(operateMysql, operateOracle, targetDir);
        }
        operateMysql.releaseResource();
        operateOracle.releaseResource();
    }

    public static Boolean importMySQLToOracle(OperateMysql operateMysql, OperateOracle operateOracle, String targetDir) {
        Boolean result = false;
        // 处理表名称并创建oracle表
        OracleInfo oracleInfo = operateOracle.getOracleInfo();
        MysqlInfo mysqlInfo = operateMysql.getMysqlInfo();
        String resultTable = DataUtil.checkTableName(oracleInfo.getOracleTable());
        // 得到oracle建表语句
        List<String> referenceTables = new ArrayList<String>();
        String createTableSQLOnOracle = operateMysql.getCreateTableSQLOnOracle(referenceTables);

        // 检查外键关联表是否存在
        for (String referenceTable : referenceTables) {
            if (!operateOracle.isExistTable(referenceTable)) {
                // 不存在外键关联表，则要先创建关联表
                MysqlInfo mysqlInfoReference = operateMysql.getMysqlInfo();
                mysqlInfoReference.setMysqlTable(referenceTable);
                operateMysql.setMysqlInfo(mysqlInfoReference);

                OracleInfo oracleInfoReference = operateOracle.getOracleInfo();
                oracleInfoReference.setOracleTable(referenceTable);
                operateOracle.setOracleInfo(oracleInfoReference);
                importMySQLToOracle(operateMysql, operateOracle, targetDir);
            }
        }

        operateOracle.createTable(resultTable, createTableSQLOnOracle);
        oracleInfo.setOracleTable(resultTable);

        // 数据从Mysql迁移到hdfs
        String SQLString = operateMysql.getSelectQuery();
        ImportToHDFS importToHDFS = new ImportToHDFS(mysqlInfo, targetDir, SQLString);
        try {
            int importResult = importToHDFS.importData();
            // 导入hdfs成功
            if (importResult == 0) {
                LOG.info("****表 " + mysqlInfo.getMysqlTable() + " 成功导出到HDFS目录" + targetDir + "****\n");
                ExportToOracle exportToOracle = new ExportToOracle(oracleInfo, targetDir);
                int exportResult = exportToOracle.exportData();
                // 导出到oracle成功
                if (exportResult == 0) {
                    LOG.info("****表 " + mysqlInfo.getMysqlTable() + " 成功导出到oracle表" + oracleInfo.getOracleTable() + "****\n");
                    // 插入成功则建立将表映射到表中
                    if (operateOracle.deleteDataFromMap(mysqlInfo.getMysqlTable())) {
                        LOG.info("****表记录 " + mysqlInfo.getMysqlTable() + " 从映射表中删除****\n");

                    }
                    if (operateOracle.addDataToMap(mysqlInfo.getMysqlTable(), resultTable)) {
                        LOG.info("****表记录 " + mysqlInfo.getMysqlTable() + " 添加到映射表****\n");

                    }
                    LOG.info("**********************************************************************************");
                    result = true;
                } else {
                    LOG.info("****表 " + mysqlInfo.getMysqlTable() + "导出到Oracle失败****");
                    result = false;
                }
            } else {
                LOG.info("****表 " + mysqlInfo.getMysqlTable() + "导出到HDFS失败****");
                result = false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }
}
```

## MySQL操作
一  MySQL操作类说明
操作类主要是实现获取相关表的外间关联表，判断相关表是否存在等操。

二  以下类主要使用JDBC来操作MySQL
```
package com.ctg.odp.collect.dbloader.importToOracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OperateMysql {
    private static final Log LOG = LogFactory.getLog(OperateMysql.class);
    private MysqlInfo mysqlInfo;
    private static String DRVIER = "com.mysql.jdbc.Driver";
    private String URL = null;
    // 创建一个数据库连接
    Connection connection = null;
    // 创建预编译语句对象，一般都是用这个而不用Statement
    PreparedStatement pstm = null;
    // 创建一个结果集对象
    ResultSet rs = null;

    public OperateMysql(MysqlInfo mysqlInfo) {
        this.mysqlInfo = mysqlInfo;
        URL = "jdbc:mysql://" + mysqlInfo.getMysqlHost() + ":3306/" + mysqlInfo.getMysqlDatabase()
                + "?useUnicode=true&amp;characterEncoding=utf-8&amp;allowMultiQueries=true&amp;zeroDateTimeBehavior=convertToNull";
        LOG.info("****开始连接到MySQL......****\n");
        connection = getConnection();
    }

    public ResultSet selectFromMysql(String table, int limitBgin, int limitEnd) {
        String sql = "select * from " + table + " limit " + limitBgin + "," + limitEnd;
        try {
            // 计算数据库表中数据总数
            System.out.println("查询语句 " + sql);
            pstm = connection.prepareStatement(sql);
            rs = pstm.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public Connection getConnection() {
        try {
            Class.forName(DRVIER);
            connection = DriverManager.getConnection(URL, mysqlInfo.getMysqlUserName(), mysqlInfo.getMysqlPassWord());
            LOG.info("****成功连接到MySql数据库****\n" + mysqlInfo.getMysqlDatabase());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("class not find !", e);
        } catch (SQLException e) {
            throw new RuntimeException("get connection error!", e);
        }

        return connection;
    }

    /**
     * 释放资源
     */
    public void releaseResource() {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstm != null) {
            try {
                pstm.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 得到表的元数据信息
    public List<String> getTableInfo() {
        List<String> colums = new ArrayList<String>();
        try {
            ResultSet colSet = connection.getMetaData().getColumns(null, "%", mysqlInfo.getMysqlTable(), "%");
            ResultSet primarySet = connection.getMetaData().getPrimaryKeys(null, "%", mysqlInfo.getMysqlTable());

            primarySet.next();
            String primaryKey = primarySet.getString(4);
            System.out.println("key=" + primaryKey);

            while (colSet.next()) {
                String COLUMN_NAME = colSet.getString("COLUMN_NAME");
                String TYPE_NAME = colSet.getString("TYPE_NAME");
                String COLUMN_SIZE = colSet.getString("COLUMN_SIZE");
                String isAutoIncrement = colSet.getString("IS_AUTOINCREMENT");
                String iS_NULLABLE = colSet.getString("IS_NULLABLE");
                StringBuilder col = new StringBuilder(COLUMN_NAME + " ");
                // 设置列的大小并且更换类型
                if ("INT".equals(TYPE_NAME)) {
                    COLUMN_SIZE = "11";
                    TYPE_NAME = "NUMBER";
                }
                if ("BIGINT".equals(TYPE_NAME)) {
                    COLUMN_SIZE = "20";
                    TYPE_NAME = "NUMBER";
                }

                if ("VARCHAR".equals(TYPE_NAME)) {
                    TYPE_NAME = "VARCHAR2";
                }

                // 添加列的名称和列的大小
                if ("DATETIME".equals(TYPE_NAME)) {
                    TYPE_NAME = "TIMESTAMP";
                    col.append(TYPE_NAME);
                } else {
                    col.append(TYPE_NAME).append("(").append(COLUMN_SIZE).append(")");
                }
                // 添加主键
                if (primaryKey.equals(COLUMN_NAME)) {
                    col.append(" PRIMARY KEY");
                }
                if ("YES".equals(isAutoIncrement)) {
                    col.append(" AUTO_INCREMENT");
                }
                if ("NO".equals(iS_NULLABLE)) {
                    col.append(" NOT NULL");
                }
                colums.add(col.toString());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return colums;
    }

    // 获取整形字段的列索引
    public List<Integer> getIntFileds() {
        List<Integer> result = new ArrayList<Integer>();
        ResultSet colSet = null;
        try {
            colSet = connection.getMetaData().getColumns(null, "%", mysqlInfo.getMysqlTable(), "%");
            while (colSet.next()) {
                String TYPE_NAME = colSet.getString("TYPE_NAME");
                if ("INT".equals(TYPE_NAME) || "BIGINT".equals(TYPE_NAME)) {
                    result.add(colSet.getInt("ORDINAL_POSITION"));
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    // 获取Date字段的列索引
    public List<Integer> getDateFileds() {
        List<Integer> result = new ArrayList<Integer>();
        ResultSet colSet = null;
        try {
            colSet = connection.getMetaData().getColumns(null, "%", mysqlInfo.getMysqlTable(), "%");
            while (colSet.next()) {
                String TYPE_NAME = colSet.getString("TYPE_NAME");
                if ("DATETIME".equals(TYPE_NAME)) {
                    result.add(colSet.getInt("ORDINAL_POSITION"));
                }

            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }


    // 计算表的总行数
    public Integer getTableCount() {
        Integer rows = 0;
        String sql = "SELECT COUNT(*) FROM " + mysqlInfo.getMysqlTable();
        try {
           pstm=connection.prepareStatement(sql);
           ResultSet resultSet=pstm.executeQuery();
            while (resultSet.next()) {
                rows = resultSet.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return rows;
    }

    // 获取在oracle建表的Sql语句
    public String getCreateTableSQLOnOracle(List<String> referenceTables) {
        String createSQL = null;
        // 获取建表sql语句
        String getCreateSQL = "SHOW CREATE TABLE " + mysqlInfo.getMysqlTable();
        try {
            pstm = connection.prepareStatement(getCreateSQL);
            ResultSet result = pstm.executeQuery();
            while (result.next()) {
                createSQL = result.getString(result.getMetaData().getColumnName(2));
            }
            createSQL = ConvertStatement.fromMySqlToOracle(createSQL, referenceTables);
        } catch (SQLException e1) {
            e1.printStackTrace();
            return null;
        }
        return createSQL;
    }

    public String getSelectQuery() {
        String selectSQL = null;
        String tableName = mysqlInfo.getMysqlTable();
        String showColQuery = "SHOW COLUMNS FROM " + tableName;
        try {
            pstm = connection.prepareStatement(showColQuery);
            ResultSet result = pstm.executeQuery();
            ArrayList<String> cols = new ArrayList<String>();
            while (result.next()) {
                String field = result.getString("Field");
                String isNull = result.getString("Null");
                if (isNull.toUpperCase().equals("NO")) {
                    cols.add("IF(" + field + "='',' '," + field + ")");
                } else {
                    cols.add(field);
                }
            }
            selectSQL = "SELECT " + StringUtils.join(cols, ",") + " FROM " + tableName + " WHERE $CONDITIONS";
        } catch (SQLException e1) {
            e1.printStackTrace();
            return null;
        }
        return selectSQL;
    }

    // 得到数据库中所有表
    public List<String> getAllTables() {
        List<String> tables = new ArrayList<String>();
        try {
            String[] types = { "TABLE" };
            ResultSet result = connection.getMetaData().getTables(null, null, null, types);
            while (result.next()) {
                tables.add(result.getString(3));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tables;
    }

    public MysqlInfo getMysqlInfo() {
        return mysqlInfo;
    }

    public void setMysqlInfo(MysqlInfo mysqlInfo) {
        this.mysqlInfo = mysqlInfo;
    }

}

```

## 操作Oracle实现
一 主要是使用JDBC来实现基本的Oracle操作

二 以下类用来操作Oracle

```
package com.ctg.odp.collect.dbloader.importToOracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OperateOracle {
    private static final Log LOG = LogFactory.getLog(OperateOracle.class);
    private OracleInfo oracleInfo;
    private static String DRVIER = "oracle.jdbc.driver.OracleDriver";
    private String URL = null;

    private String mapTable = null;
    private String sequenceName = null;

    // 创建一个数据库连接
    Connection connection = null;
    // 创建预编译语句对象，一般都是用这个而不用Statement
    PreparedStatement pstm = null;
    // 创建一个结果集对象
    ResultSet rs = null;

    public OperateOracle(OracleInfo oracleInfo) {
        this.oracleInfo = oracleInfo;
        URL = "jdbc:oracle:thin:@" + oracleInfo.getOracleHost() + ":1521:" + oracleInfo.getOracleDatabase();
        LOG.info("****开始连接到Oracle......****\n");
        connection = getConnection();
    }

    public OperateOracle(OracleInfo oracleInfo, String mapTable, String sequenceName) {
        this.mapTable = mapTable;
        this.sequenceName = sequenceName;
        this.oracleInfo = oracleInfo;
        URL = "jdbc:oracle:thin:@" + oracleInfo.getOracleHost() + ":1521:" + oracleInfo.getOracleDatabase();
        LOG.info("****开始连接到Oracle****\n");
        connection = getConnection();
    }

    // 检查表是否存在
    public Boolean isExistTable(String table) {
        String checkSql = "select count(*) from USER_OBJECTS where OBJECT_NAME = " + "'" + table.toUpperCase() + "'";
        LOG.info("****检查表 " + table + " 是否已经存在****\n");
        try {
            pstm = connection.prepareStatement(checkSql);
            ResultSet result = pstm.executeQuery();
            result.next();
            if (result.getInt(1) == 1) {
                LOG.info("****表  " + table + " 已经存在****\n");
                return true;
            } else {
                return false;
            }
        } catch (SQLException e1) {
            e1.printStackTrace();
            return false;
        }
    }

    // 根据给出的表名称和列的相关信息创建表
    public boolean createTable(String resultTable, String createTableSQLOnOracle) {
        if (isExistTable(resultTable)) {
            return false;
        } else {
            // 在oracle中建表
            createTableSQLOnOracle = createTableSQLOnOracle.replace(oracleInfo.getOracleTable(), resultTable);
            createTableSQLOnOracle = createTableSQLOnOracle.replace(oracleInfo.getOracleTable().toUpperCase(), resultTable);
            LOG.info("****开始建表，建表语句为****\n" + createTableSQLOnOracle);
            try {
                String[] SQLArray = createTableSQLOnOracle.split(";");
                for (String sql : SQLArray) {
                    sql = sql.trim();
                    if (null != sql && !"\n".equals(sql) && !"".equals(sql)) {
                        pstm = connection.prepareStatement(sql);
                        int result = pstm.executeUpdate();
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return false;
            }

            oracleInfo.setOracleTable(resultTable);
            LOG.info("****成功创建表****\n" + resultTable);
            return true;
        }
    }

    // 删除映射表中数据
    public boolean deleteDataFromMap(String oldTable) {
        String insertSQL = "delete from " + mapTable + " where old_table=?";
        try {
            pstm = connection.prepareStatement(insertSQL);
            pstm.setString(1, oldTable);
            int result = pstm.executeUpdate();
            return result == 1 ? true : false;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    // 向映射表中添加数据
    public boolean addDataToMap(String oldTable, String newTable) {
        String insertSQL = "insert into " + mapTable + " values(" + sequenceName + ".NEXTVAL,?,?)";
        try {
            pstm = connection.prepareStatement(insertSQL);
            pstm.setString(1, oldTable);
            pstm.setString(2, newTable);
            int result = pstm.executeUpdate();
            return result == 1 ? true : false;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    // 检查序列是否存在
    public Boolean isExistSequence(String sequence) {
        LOG.info("****检查序列 " + sequence + " 是否存在****");
        String checkSQL = "select count(*) from user_sequences  where  sequence_name='" + sequenceName.toUpperCase() + "'";
        try {
            pstm = connection.prepareStatement(checkSQL);
            ResultSet result = pstm.executeQuery();
            result.next();
            return result.getInt(1) == 1 ? true : false;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    // 创建映射表
    @SuppressWarnings("finally")
    public Boolean createMapTable() {
        Boolean result = false;
        String createTabkleSQL = "CREATE TABLE " + mapTable + "(id NUMBER PRIMARY KEY,old_table VARCHAR2(100),new_table VARCHAR2(100)  )";
        String SequenceSQL = "CREATE SEQUENCE " + sequenceName + " INCREMENT BY 1  START WITH 1  MINVALUE 1 NOMAXVALUE NOCYCLE NOCACHE";

        if (!isExistTable(mapTable))
            try {
                pstm = connection.prepareStatement(createTabkleSQL);
                int exeResultA = pstm.executeUpdate();
                if (exeResultA == 0) {
                    LOG.info("****成功创建映射表表****\n" + mapTable);
                    if (!isExistSequence(sequenceName)) {
                        pstm = connection.prepareStatement(SequenceSQL);
                        int exeResultB = pstm.executeUpdate();
                        if (exeResultB == 0) {
                            LOG.info("****成功创建序列****\n" + sequenceName);
                        }
                    } else {
                        LOG.info("****序列 " + sequenceName + " 已经存在****\n");
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        return result;
    }

    public Connection getConnection() {
        try {
            Class.forName(DRVIER);
            System.out.println(URL);
            connection = DriverManager.getConnection(URL, oracleInfo.getOracleUserName(), oracleInfo.getOraclePassWord());
            LOG.info("****成功连接到Oracle数据库****\n" + oracleInfo.getOracleDatabase());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("class not find !", e);
        } catch (SQLException e) {
            throw new RuntimeException("get connection error!", e);
        }

        return connection;
    }

    /**
     * 释放资源
     */
    public void releaseResource() {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (pstm != null) {
            try {
                pstm.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public OracleInfo getOracleInfo() {
        return oracleInfo;
    }

    public void setOracleInfo(OracleInfo oracleInfo) {
        this.oracleInfo = oracleInfo;
    }

    public String getMapTable() {
        return mapTable;
    }

    public void setMapTable(String mapTable) {
        this.mapTable = mapTable;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

}


```


## 将数据从MySQL迁移到HDFS
一 主要功能：
将数据从MySQL迁移到HDFS,形成文本文件

二 以下是代码实现
```
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
        options.setDeleteMode(true);
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

```

## 将数据HDFS迁移到Oracle
一 主要功能：将数据从HDFS迁移到Oracle

二 以下是实现代码

```

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

```

# 数据切割测试
---

## 测试环境准备
1：Mysql服务，准备一张用作数据导入的数据表。这里使用表dbload_task_instance_run_result

2：Oracle服务，开启Oracle服务。

3：开启Hadoop服务。
## 测试步骤
因为主类中已将数据的建表，导入，数据导入到HDFS，数据导出到Oracle这几个过程进行了集成。所以这里只需以Java Application运行程序即可。然后我们可以查看运行结果。
## 测试结果
1：在Hadoop上可以查看目标文件内容，如下：
```
1^46^增量参照列名称^Integer^1234
1^91^qw^Integer^12
1^92^3^Integer^3
2^1^id^int^0
2^2^creattime^timestamp^2016-03-23 00:00:0.0
3^5^id^int^0
3^6^creattime^timestamp^2016-03-23 00:00:12.0
3^69^增量采集^Integer^2
42^51^creattime^timestamp^2016-03-23 00:00:12.0
47^64^qw^Date^21
47^65^wqwq^Integer^NaN
47^66^name^Integer^2
47^70^re^Integer^23
47^71^testParamsItem003^Date^1471190400000
51^62^id^Integer^NaN
58^75^id^Integer^12789499
58^76^creattime^Date^1458662405000
60^78^id^Integer^12789499
60^80^id^Integer^12789499
63^81^id^Integer^0
65^82^mdn^Integer^1
73^89^task_item_id^Integer^92
74^93^task_item_id^Integer^95
82^95^增量采集列名称^Integer^12
88^98^task_id^Integer^93
91^103^col^Integer^112

```
上图中字符^为程序中设置的字段分隔符（默认为英文逗号）。

2：查看结果：

在Oracle中也可以看到实际对应的表。

JackerWang 于2017年秋天（10月26日）下午的广州

----
[个人技术站点](https://jacker-wang.github.io/)
