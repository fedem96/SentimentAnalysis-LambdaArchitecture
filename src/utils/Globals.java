package utils;

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
}
