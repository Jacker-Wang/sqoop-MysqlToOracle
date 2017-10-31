package com.ctg.odp.collect.dbloader.importToOracle;

import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.xml.crypto.Data;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DataUtil {
    private static final Log log = LogFactory.getLog(DataUtil.class);

    // 将ResultSet中的每一行放进数组中
    public static List<String> resultSetToList(ResultSet resultSet) {
        List<String> result = new ArrayList<String>();
        try {
            // 得到结果集的列数
            int columnCount = resultSet.getMetaData().getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                result.add(resultSet.getString(i));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    // 使用命令行选项
    public static CommandLine getCommandLine(String[] args) {
        Options options = new Options();
        options.addOption(new Option("fh", "fromHost", true, "mysql dataBaseHost"));
        options.addOption(new Option("fd", "fromDataBase", true, "mysql dataBase"));
        options.addOption(new Option("fu", "fromUser", true, "mysql userName"));
        options.addOption(new Option("fp", "fromPassWord", true, "mysql passWord"));
        options.addOption(new Option("ft", "fromTable", true, "mysql table"));

        options.addOption(new Option("th", "toHost", true, "oracle Host"));
        options.addOption(new Option("td", "toDataBase", true, " oracle Database"));
        options.addOption(new Option("tu", "toUserName", true, " oracle userName"));
        options.addOption(new Option("tp", "toPassWord", true, " oracle passWord"));

        // create the command line parser
        @SuppressWarnings("deprecation")
        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return cmd;
    }

    // 读取配置文件参数
    public static String getField(String field) {
        String result = null;
        Properties properties = new Properties();
        InputStream inputStream = Data.class.getResourceAsStream("/properties");
        try {
            properties.load(inputStream);
            result = properties.getProperty(field);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return result;
        }
    }

    // 根据命令行参数包装mysql信息
    public static MysqlInfo getMysqlInfo(CommandLine commandLine) {
        String mysqlHost = commandLine.hasOption("fh") ? commandLine.getOptionValue("fh") : getField("MySQLHost");
        String mysqlDatabase = commandLine.hasOption("fd") ? commandLine.getOptionValue("fd") : getField("MySQLDatabase");
        String mysqlUserName = commandLine.hasOption("fu") ? commandLine.getOptionValue("fu") : getField("MySQLUsername");
        String mysqlPassWord = commandLine.hasOption("fp") ? commandLine.getOptionValue("fp") : getField("MySQLPassword");
        String mysqlTable = commandLine.hasOption("ft") ? commandLine.getOptionValue("ft") : getField("MySQLTable");
        MysqlInfo mysqlInfo = new MysqlInfo(mysqlHost, mysqlDatabase, mysqlUserName, mysqlPassWord, mysqlTable);
        return mysqlInfo;
    }

    // 根据命令行参数包装oracle信息
    public static OracleInfo getOracleInfo(CommandLine commandLine) {
        String oracleHost = commandLine.hasOption("th") ? commandLine.getOptionValue("th") : getField("OracleHost");
        String oracleDatabase = commandLine.hasOption("td") ? commandLine.getOptionValue("td") : getField("OracleDatabase");
        String oracleUsername = commandLine.hasOption("tu") ? commandLine.getOptionValue("tu") : getField("OracleUsername");
        String oraclePassWord = commandLine.hasOption("tp") ? commandLine.getOptionValue("tp") : getField("OraclePassword");
        String oracleTable = commandLine.hasOption("tt") ? commandLine.getOptionValue("tt") : getMysqlInfo(commandLine).getMysqlTable();
        oracleTable = oracleTable == null ? null : oracleTable;
        OracleInfo oracleInfo = new OracleInfo(oracleHost, oracleDatabase, oracleUsername, oraclePassWord, oracleTable);
        return oracleInfo;
    }

    // 检查表名的长度
    public static String checkTableName(String table) {
        String tableName = table;
        // 检查表名长度是否越界
        if (table.length() > 23) {
            log.warn("****你的表名长度大于最大大小，请注意!!!，表名有所更改****\n");
            tableName = table.substring(0, 23);
        }
        return tableName;
    }

    // 检查是否是合格的日期格式
    public static boolean checkIsDate(String date) {
        Boolean convertSuccess = true;
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            format.setLenient(false);
            format.parse(date);
        } catch (Exception e) {
            // e.printStackTrace();
            // 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
            convertSuccess = false;
        }
        return convertSuccess;
    }

    // 处理日期字段
    public static String handleDate(String date) {
        String result = date;
        if (checkIsDate(date) && date.contains(".")) {
            int index = date.indexOf(".");
            result = date.substring(0, index);
            result = "to_date('" + result + "','YYYY-MM-DD HH24:MI:SS')";
        }
        return result;
    }
}
