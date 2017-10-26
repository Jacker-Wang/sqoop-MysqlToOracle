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
        LOG.info("****开始连接到Oracle****\n");
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
