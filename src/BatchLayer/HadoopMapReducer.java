package BatchLayer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;


public class HadoopMapReducer {

    //TODO here make the executable that uses map and reduce
    public int run(String[] args) throws Exception {


        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Map(), args);
        System.exit(res);
    }

}
