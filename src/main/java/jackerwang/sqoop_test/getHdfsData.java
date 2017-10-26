package jackerwang.sqoop_test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class getHdfsData {

    public static void main(String[] args) throws IOException {
        String uri = "hdfs://localhost:9000/input";
        Configuration cfg = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), cfg);
        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
