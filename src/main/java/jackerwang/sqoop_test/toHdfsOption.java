package jackerwang.sqoop_test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.tool.ImportTool;

import com.cloudera.sqoop.SqoopOptions;

public class toHdfsOption {

    public static void main(String args[]) {
        SqoopOptions options;
        try {
            options = getSqoopOptions();
            ImportTool importTool = new ImportTool();
            int result = importTool.run(options);
            System.out.println(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static SqoopOptions getSqoopOptions() throws IOException {
        Configuration conf = new Configuration();
        SqoopOptions options = new SqoopOptions(conf);
        ImportTool importTool = new ImportTool();
        options.setActiveSqoopTool(importTool);
        // options.setDeleteMode(true);

        // Mysql
        options.setConnectString("jdbc:mysql://132.122.1.192:3306/odp");
        options.setTableName("dbload_task_item_instance_run_result");
        options.setUsername("dbsync");
        options.setPassword("dbsync");
        options.setDriverClassName("com.mysql.jdbc.Driver");
        options.setNullNonStringValue("");

        options.setTargetDir("hdfs://localhost:9000/hadoop");
        System.out.println("***sqoopOptions****");
        System.out.println("sql=" + options.getSqlQuery());
        System.out.println("targetDir=" + options.getTargetDir());
        System.out.println("numMappers=" + options.getNumMappers());
        return options;
    }
}
