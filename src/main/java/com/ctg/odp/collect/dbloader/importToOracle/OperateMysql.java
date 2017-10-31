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
        URL = "jdbc:mysql://" + mysqlInfo.getMysqlHost() + ":8108/" + mysqlInfo.getMysqlDatabase()
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
            System.out.println(URL);
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
            pstm = connection.prepareStatement(sql);
            ResultSet resultSet = pstm.executeQuery();
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
