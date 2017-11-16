package com.ctg.odp.collect.dbloader.importToOracle;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ImportDataTableList {
    private static String mapTable = null;
    private static String sequence = null;
    private static CommandLine commandLine = null;
    private static String targetDir = null;
    private static final Log LOG = LogFactory.getLog(ImportDataTableList.class);
    private static MysqlInfo mysqlInfo = null;
    private static OracleInfo oracleInfo = null;

    private static OperateMysql operateMysql = null;
    private static OperateOracle operateOracle = null;

    // 缓存映射到oracle中新的表名称
    private static List<String> oracleTables = new ArrayList<String>();

    private static String tablesFilePath = "C:\\Users\\Pin-Wang\\Desktop\\tables.txt";

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

        List<String> tables = TablesListUtil.getTables(tablesFilePath);
        // tables.clear();
        // tables.add("i_prod_offr_inst".toLowerCase());
        int oracleCursorCount = 0;
        for (String table : tables) {
            if (oracleCursorCount > 10) {
                oracleCursorCount = 0;
                operateMysql.releaseResource();
                operateOracle.releaseResource();
                operateMysql = new OperateMysql(mysqlInfo);
                operateOracle = new OperateOracle(oracleInfo, mapTable, sequence);
                // 建立索引表
                operateOracle.createMapTable();
            }

            mysqlInfo.setMysqlTable(table);
            oracleInfo.setOracleTable(table);
            operateMysql.setMysqlInfo(mysqlInfo);
            operateOracle.setOracleInfo(oracleInfo);
            importMySQLToOracle(operateMysql, operateOracle, "");
            oracleCursorCount++;
        }

        // System.out.println("没有主键");
        // for (String table : ConvertStatement.getNoPrimaryKeyList()) {
        // System.out.println(table);
        // }

        // if (mysqlInfo.getMysqlTable() == null) {
        // List<String> tablesList = operateMysql.getAllTables();
        // for (String table : tablesList) {
        // LOG.info("****开始导入数据表****\n" + table);
        // mysqlInfo.setMysqlTable(table);
        // oracleInfo.setOracleTable(table);
        // operateOracle.setOracleInfo(oracleInfo);
        // operateMysql.setMysqlInfo(mysqlInfo);
        // importMySQLToOracle(operateMysql, operateOracle, targetDir);
        // }
        // } else {
        // LOG.info("****开始导入数据表****\n" + mysqlInfo.getMysqlTable());
        // importMySQLToOracle(operateMysql, operateOracle, targetDir);
        // }
    }

    public static Boolean importMySQLToOracle(OperateMysql operateMysql, OperateOracle operateOracle, String targetDir) {
        Boolean result = false;
        // 处理表名称并创建oracle表
        OracleInfo oracleInfo = operateOracle.getOracleInfo();
        MysqlInfo mysqlInfo = operateMysql.getMysqlInfo();
        String resultTable = DataUtil.checkTableName(oracleInfo.getOracleTable());

        // 初始化oracleTables
        operateOracle.getMapNewTables(oracleTables);

        // oracle中已经存在此表，则更换表名称
        // resultTable = DataUtil.handleTableName(oracleTables, resultTable);

        // 得到oracle建表语句
        List<String> referenceTables = new ArrayList<String>();
        String createTableSQLOnOracle = operateMysql.getCreateTableSQLOnOracle(referenceTables);

        // MySQL不存在此表
        if (null == createTableSQLOnOracle) {
            LOG.info("****获取表 " + mysqlInfo.getMysqlTable() + " 的建表语句失败 ****\n");
            return false;
        }

        // 检查外键关联表是否存在
        // for (String referenceTable : referenceTables) {
        // if (!operateOracle.isExistTable(referenceTable)) {
        // // 不存在外键关联表，则要先创建关联表
        // MysqlInfo mysqlInfoReference = operateMysql.getMysqlInfo();
        // mysqlInfoReference.setMysqlTable(referenceTable);
        // operateMysql.setMysqlInfo(mysqlInfoReference);
        //
        // OracleInfo oracleInfoReference = operateOracle.getOracleInfo();
        // oracleInfoReference.setOracleTable(referenceTable);
        // operateOracle.setOracleInfo(oracleInfoReference);
        // importMySQLToOracle(operateMysql, operateOracle, targetDir);
        // }
        // }

        if (operateOracle.createTable(resultTable, createTableSQLOnOracle)) {
            if (operateOracle.deleteDataFromMap(oracleInfo.getOracleTable())) {
                LOG.info("****表记录 " + mysqlInfo.getMysqlTable() + " 从映射表中删除****\n");

            }
            if (operateOracle.addDataToMap(mysqlInfo.getMysqlTable(), resultTable)) {
                LOG.info("****表记录 " + mysqlInfo.getMysqlTable() + " 添加到映射表****\n");

            }
            LOG.info("**********************************************************************************");
        }
        oracleInfo.setOracleTable(resultTable);

        /*
         * // 数据从Mysql迁移到hdfs String SQLString = operateMysql.getSelectQuery();
         * ImportToHDFS importToHDFS = new ImportToHDFS(mysqlInfo, targetDir,
         * SQLString); try { int importResult = importToHDFS.importData(); //
         * 导入hdfs成功 if (importResult == 0) { LOG.info("****表 " +
         * mysqlInfo.getMysqlTable() + " 成功导出到HDFS目录" + targetDir + "****\n");
         * ExportToOracle exportToOracle = new ExportToOracle(oracleInfo,
         * targetDir); int exportResult = exportToOracle.exportData(); //
         * 导出到oracle成功 if (exportResult == 0) { LOG.info("****表 " +
         * mysqlInfo.getMysqlTable() + " 成功导出到oracle表" +
         * oracleInfo.getOracleTable() + "****\n"); // 插入成功则建立将表映射到表中 if
         * (operateOracle.deleteDataFromMap(mysqlInfo.getMysqlTable())) {
         * LOG.info("****表记录 " + mysqlInfo.getMysqlTable() + " 从映射表中删除****\n");
         * 
         * } if (operateOracle.addDataToMap(mysqlInfo.getMysqlTable(),
         * resultTable)) { LOG.info("****表记录 " + mysqlInfo.getMysqlTable() +
         * " 添加到映射表****\n");
         * 
         * } LOG.info(
         * "**********************************************************************************"
         * ); result = true; } else { LOG.info("****表 " +
         * mysqlInfo.getMysqlTable() + "导出到Oracle失败****"); result = false; } }
         * else { LOG.info("****表 " + mysqlInfo.getMysqlTable() +
         * "导出到HDFS失败****"); result = false; } } catch (IOException e) {
         * e.printStackTrace(); }
         */
        return result;
    }
}
