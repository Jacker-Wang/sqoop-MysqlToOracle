package jackerwang.sqoop_test;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.Sqoop;
import org.apache.sqoop.tool.SqoopTool;
import org.apache.sqoop.util.OptionsFileUtil;


public class toHdfs {
    private static int importDataFromMysql() throws Exception {
        String[] args = new String[] { "--connect", "jdbc:mysql://localhost:3306/odp", "--driver", "com.mysql.jdbc.Driver", "--username", "root", "--password",
                "1234", "--table", "dbload_db", "-m", "1", "--target-dir", "dbload_db" };

        String[] expandArguments = OptionsFileUtil.expandArguments(args);
        com.cloudera.sqoop.tool.SqoopTool tool = (com.cloudera.sqoop.tool.SqoopTool) SqoopTool.getTool("import");

        Configuration conf = new Configuration();
        conf.set("fs.default.name", "hdfs://localhost:9000");
        // 设置HDFS服务地址
        Configuration loadPlugins = SqoopTool.loadPlugins(conf);

        Sqoop sqoop = new Sqoop(tool, loadPlugins);
        int result = Sqoop.runSqoop(sqoop, expandArguments);
        System.out.println(result);
        return result;
    }

    public static void main(String[] args) throws Exception {
        importDataFromMysql();
    }

}
