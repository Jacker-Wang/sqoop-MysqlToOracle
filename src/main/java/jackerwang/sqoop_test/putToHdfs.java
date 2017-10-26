package jackerwang.sqoop_test;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class putToHdfs {

    public static void main(String[] args) throws Exception {
        // 本地文件路径
        String local = "C:\\Users\\Pin-Wang\\Desktop\\input.txt";
        String dest = "hdfs://localhost:9000/user/wcinput/word2.txt";
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dest), cfg, "root");
        fs.copyFromLocalFile(new Path(local), new Path(dest));
        fs.close();
    }
}
