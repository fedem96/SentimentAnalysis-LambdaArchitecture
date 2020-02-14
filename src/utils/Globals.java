package utils;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class Globals {

    public static final String hdfsURI = "hdfs://localhost:9000";

    public static String datePattern = "dd-MMM-yyyy";

    public static final String batchInputPath = "/batch/input";
    public static final String[] batchOutputPaths = new String[]{"/batch/output0", "/batch/output1"};
//    public static final String batchOutputFile = batchOutputPath + "/part-r-00000";

    public static final String speedInputPath = "/speed/input";
    public static final String speedOutputPath = "/speed/output";
    public static String speedArchivePath = "/speed/archive";
    public static String speedBadFiles = "/speed/bad";

    public static final String syncPath = "/sync";
    public static final String syncProgressTimestamp = syncPath + "/progress_timestamp.txt";
    public static final String syncLastBatchOutput = syncPath + "/last_batch_output.txt";

    public static void writeStringToHdfsFile(FileSystem fs, String string, String filePath) throws IOException {
        FSDataOutputStream outputStream = fs.create(new Path(Globals.syncProgressTimestamp));
        outputStream.writeChars(string);
        outputStream.close();
    }

    public static String readStringFromHdfsFile(FileSystem fs, String filePath){
        Path hdfsPath = new Path(filePath); //Create a path
        FSDataInputStream is = null; //Init input stream
        String str = null;
        try {
            is = fs.open(hdfsPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            str = IOUtils.toString(is, "UTF-16");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return str;
    }
}
