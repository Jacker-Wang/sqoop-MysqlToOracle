package jackerwang.sqoop_test;

import java.io.IOException;

import org.apache.sqoop.tool.ExportTool;

import com.cloudera.sqoop.SqoopOptions;

public class ExportToOracle {

    public static void main(String args[]) {
        SqoopOptions options;
        try {
            options = getSqoopOptions();
            ExportTool exportTool = new ExportTool();
            int result = exportTool.run(options);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @SuppressWarnings("deprecation")
    public static SqoopOptions getSqoopOptions() throws IOException {
        // H3
        String dir = "hdfs://localhost:9000/hadoop";
        // 本地hadoop
        // String dir = "hdfs://localhost:9000/hadoop";
        SqoopOptions options = new SqoopOptions();
        ExportTool exportTool = new ExportTool();
        options.setActiveSqoopTool(exportTool);
        options.setVerbose(true);

        options.setConnectString("jdbc:oracle:thin:@132.122.1.163:1521:orcl2");
        options.setTableName("DBLOAD_TASK_ITEM_INSTANCE_RUN_");
        options.setUsername("icrm");
        options.setPassword("icrm");
        options.setDirectMode(true);
        options.setExportDir(dir);
        // options.setDriverClassName("oracle.jdbc.driver.OracleDriver");
        options.setConnManagerClassName("org.apache.sqoop.manager.OracleManager");

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

