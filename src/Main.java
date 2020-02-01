import batch.BatchLayer;
import generate.Generator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class Main {

    public static void main(String[] args) throws Exception{

        if(args.length <= 0){
            System.err.println("Error: not enough args");
            return;
        }

        final String datasetPath = args[0];
        final String batchInputPath = "/batch/input";
        final String speedInputPath = "/speed/input";
        final String batchOutputPath = "/batch/output";
        final String speedOutputPath = "/speed/output";

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // delete dirs
        for(String path : new String[]{batchInputPath, speedInputPath, batchOutputPath, speedOutputPath})
            if (fs.exists(new Path(path)))  // TODO fix: exists is always false
                fs.delete(new Path(path), true);

        // create and start Generator thread
        new Generator("hdfs://localhost:9000", datasetPath, batchInputPath, speedInputPath).start();

        // create and start Batch Layer
        Job batchLayer = new BatchLayer(conf, "BatchTwitterSentimentAnalysis", batchInputPath, batchOutputPath);
        batchLayer.setJarByClass(Main.class); // TODO check if correct
        batchLayer.waitForCompletion(true);

        // create and start Speed Layer
        // TODO implement

        // create and start Serving Layer
        // TODO implement
    }

}
