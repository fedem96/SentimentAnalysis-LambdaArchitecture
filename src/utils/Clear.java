package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Clear {


    public static void main(String args[]) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", Globals.hdfsURI);
        FileSystem fs = FileSystem.get(conf);

        for(String strPath : new String[]{Globals.batchInputPath, Globals.speedInputPath, Globals.batchOutputPath, Globals.speedOutputPath}) {
            Path path = new Path(strPath);
            if (fs.exists(path)) {
                fs.delete(path, true);
                System.out.println(strPath + " deleted");
            }
        }
    }
}
