package com.ctg.odp.collect.dbloader.importToOracle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class ConvertStatement {

    private static final Logger logger = Logger.getLogger(ConvertStatement.class);

    /**
     * Converts a statement from MySql to Oracle
     * 
     * @param statement
     *            The MySql statement to convert
     * @return The statement as Oracle
     */
    public static String fromMySqlToOracle(String statement, List<String> referenceTables) {
        TreeMap<String, String> replacements = new TreeMap<String, String>();
        replacements.put("`", "");
        replacements.put("VARCHAR", "VARCHAR2");
        replacements.put("(?<=\\s+)TEXT(?=\\s*)", "VARCHAR(1000) ");
        replacements.put("NOT NULL", "NOT NULL ENABLE");
        replacements.put(" INT ", " NUMBER ");
        replacements.put(" SMALLINT ", " NUMBER ");
        replacements.put(" BIGINT ", " NUMBER ");
        replacements.put("TINYINT", "NUMBER");
        replacements.put("ENGINE = \\w*.", ";");// remove the engine type
        replacements.put("IF NOT EXISTS .\\w*.\\.", "");
        replacements.put(" DATETIME ", " DATE ");
        replacements.put(" TIMESTAMP ", " DATE ");

        replacements.put(" TIME ", " VARCHAR2(20) ");

        // replacements.put("DEFAULT NULL", "");

        replacements.put(" INT\\(\\d*\\) ", " NUMBER ");
        replacements.put(" SMALLINT\\(\\d*\\) ", " NUMBER ");
        replacements.put(" BIGINT\\(\\d*\\) ", " NUMBER ");

        replacements.put("ON DELETE NO ACTION", "");
        replacements.put("ON UPDATE NO ACTION", "");
        replacements.put(".mydb..", "");
        replacements.put("CREATE SCHEMA IF NOT EXISTS DEFAULT CHARACTER", "");
        replacements.put("\\s*?\\)", ")");// mysql work bench seems to put some
                                          // anoying white space before closing
                                          // some brackets
        replacements.put("IF NOT EXISTS", "");// mysql work bench seems to put
                                              // some anoying white space before
                                              // closing some brackets
        replacements.put("AUTO_INCREMENT", "");

        replacements.put("\"\\w*\"\\.", "");// remove the engine type

        // remove the crap at the top for new database setup
        // replacements.put("CREATE SCHEMA.*?;","");//remove the engine type
        replacements.put("USE.*?;", "");// remove the engine type
        replacements.put("SET.*?;", "");// remove the engine type
        replacements.put("\\;", "/");

        String oracleStatement = statement.trim();

        for (Map.Entry<String, String> vkPairs : replacements.entrySet()) {
            oracleStatement = oracleStatement.replaceAll("(?is)" + vkPairs.getKey(), vkPairs.getValue());
        }

        String lowerStatement = oracleStatement.toLowerCase();

        if (lowerStatement.startsWith("alter") && lowerStatement.indexOf("after ") != -1) {
            int index = lowerStatement.lastIndexOf("after ");
            oracleStatement = oracleStatement.substring(0, index - 1);
        }

        if (lowerStatement.startsWith("alter") && lowerStatement.indexOf("change ") != -1) {
            String[] values = oracleStatement.split(" ");
            logger.info("oracleStatement : " + oracleStatement);
            logger.info("values length : " + values.length);
            if (values.length >= 6) {
                String tableName = values[2];
                String oldCol = values[4];
                String newCol = values[5];
                String other = "";
                if (values.length > 6) {
                    other = oracleStatement.substring(oracleStatement.indexOf(newCol + " ") + newCol.length());
                }
                StringBuffer sb = new StringBuffer("ALTER TABLE ");
                sb.append(tableName);
                sb.append(" RENAME COLUMN ");
                sb.append(oldCol);
                sb.append(" TO ");
                sb.append(newCol);
                if (!StringUtils.isBlank(other)) {
                    other = other.replaceAll("(?is)NOT NULL ENABLE", "").replaceAll("(?is)DEFAULT NULL", "").replaceAll("(?is)NULL", "");
                    sb.append(";ALTER TABLE ");
                    sb.append(tableName);
                    sb.append(" MODIFY (");
                    sb.append(newCol + " ");
                    sb.append(other + ")");
                }
                oracleStatement = sb.toString();
            }
        }

        // 替换联合索引的逗号，为更好匹配正则表达式
        oracleStatement = oracleStatement.replaceAll(",(?=([\\w_,]*\\)))", "^");

        System.out.println("原始oracleStatement\n " + oracleStatement);
        if (lowerStatement.startsWith("create")) {
            Pattern pattern = Pattern.compile("(?is)(?<=\\().*(?=\\))");
            Matcher matcher = pattern.matcher(oracleStatement);
            while (matcher.find()) {
                String head = oracleStatement.substring(0, matcher.start());
                String bodys[] = matcher.group().split(",(?=([^'\"]*('|\")[^'\"]*('|\"))*[^'\"]*$)");
                String tail = oracleStatement.substring(matcher.end());
                String tableName = head.split(" ")[2];
                List<String> primaryKey = new ArrayList<String>();
                ArrayList<String> extraList = new ArrayList<String>();
                ArrayList<String> bodyList = new ArrayList<String>();
                for (String body : bodys) {
                    body = body.replaceAll("\\^", ",");
                    System.out.println(body);
                    body = body.trim();
                    String colName = body.split("\\s+")[0];
                    // 修改默认时间格式
                    body = body.replace("'0000-00-00 00:00:00'", "to_date('2017','YYYY')");
                    body = body.replace("CURRENT_TIMESTAMP", "SYSDATE");
                    // 去掉字符集和校对规则
                    body = body.replace(" CHARACTER SET utf8 COLLATE utf8_bin", "");
                    // 去掉double类型的默认值为NULL
                    body = body.replace("double DEFAULT NULL", "NUMBER DEFAULT NULL");
                    // 检查VARCHAR2的长度
                    Pattern patternVarLength = Pattern.compile("(?is)(?<=VARCHAR2\\()\\d+(?=\\))");
                    Matcher patternVarMather = patternVarLength.matcher(body);
                    while (patternVarMather.find()) {
                        Integer length = Integer.valueOf(patternVarMather.group());
                        length = length > 4000 ? 4000 : length;
                        body = body.replace(patternVarMather.group(), String.valueOf(length));
                    }
                    // 解决uresignd字段
                    if (body.contains("unsigned")) {
                        String constraintSQL = "constraint " + tableName + "_" + colName + " check (" + colName + " between 0 and 4294967295)";
                        body = body.replace("unsigned", "");
                        bodyList.add(constraintSQL);
                    }

                    // 调换默认值和是否为空表示的位置
                    if (body.contains("DEFAULT") && body.contains("NOT NULL ENABLE")) {
                        String[] temArr = body.split("\\s+");
                        // 过滤掉列名称含有DEFAULT的
                        if (!temArr[0].contains("DEFAULT")) {
                            int defaultIndex = Arrays.asList(temArr).indexOf("DEFAULT");

                            String DefaultValue = temArr[defaultIndex + 1].trim();
                            System.out.println("DefaultValue " + DefaultValue);
                            DefaultValue = "DEFAULT " + DefaultValue;
                            body = body.replace(DefaultValue, "NOT NULL ENABLE");
                            body = body.replaceFirst("NOT NULL ENABLE", DefaultValue);
                        }
                    }

                    // 处理自动更新的DATE类型
                    if (body.contains("ON UPDATE SYSDATE")) {
                        body = body.replace("ON UPDATE SYSDATE", "");
                        String createTriggerSQL = "ON UPDATE SYSDATE " + tableName + " " + body.split("\\s+")[0];
                        extraList.add(createTriggerSQL);
                    }
                    if (body.startsWith("PRIMARY KEY")) {// 主键或外键
                        primaryKey.add(body.split("\\s+")[2]);
                        bodyList.add(body);
                        continue;
                    } else if (body.startsWith("CONSTRAINT") && body.contains("FOREIGN KEY")) {
                        String[] values = body.split("\\s+");
                        referenceTables.add(values[6]);
                        bodyList.add(body);
                    } else if (body.startsWith("UNIQUE KEY")) {// 唯一索引
                        String[] values = body.split("\\s+");
                        if (!primaryKey.contains(values[3])) {
                            extraList.add("CREATE UNIQUE INDEX " + values[2] + " ON " + tableName + values[3]);
                        }
                        continue;
                    } else if (body.startsWith("KEY")) { // 普通索引
                        String[] values = body.split("\\s+");
                        extraList.add("CREATE INDEX " + values[1] + " ON " + tableName + values[2]);
                        continue;
                    } else if (body.contains(" COMMENT ")) {// 处理注释
                        String[] values = body.split(" (?=([^'\"]*('|\")[^'\"]*('|\"))*[^'\"]*$)");
                        String column = values[0];
                        String comment = values[values.length - 1];
                        extraList.add("COMMENT ON COLUMN " + tableName + "." + column + " is " + comment);
                        String resultBody = body.replaceAll("(?is)COMMENT\\s+\\'.*?\\'", "");
                        bodyList.add(resultBody);
                    } else {
                        bodyList.add(body);
                    }
                }
                if (tail.contains("COMMENT")) {
                    pattern = Pattern.compile("(?is)\'[^\']*\'");
                    matcher = pattern.matcher(tail);
                    while (matcher.find()) {
                        extraList.add("COMMENT ON TABLE " + tableName + " IS " + matcher.group());
                    }
                }
                // 拼接CREATE语句
                oracleStatement = head + StringUtils.join(bodyList, ",") + ")";
                if (extraList.size() > 0) {
                    oracleStatement = oracleStatement + ";" + StringUtils.join(extraList, ";");
                }
            }
        }
        return oracleStatement;
    }
}
