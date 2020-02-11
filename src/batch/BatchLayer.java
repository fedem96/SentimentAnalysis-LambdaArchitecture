package batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import utils.Globals;

import java.io.IOException;

public class BatchLayer extends Job {


    public BatchLayer(Configuration conf, String jobName, String batchInputPath, String batchOutputPath) throws IOException {
        super(conf, jobName);

        this.setMapperClass(Map.class);
        this.setReducerClass(Reduce.class);

        this.setMapOutputKeyClass(Text.class);           // timestamp
        this.setMapOutputValueClass(IntWritable.class);  // sentiment

        this.setOutputKeyClass(Text.class);              // timestamp
        this.setOutputValueClass(Text.class);         // <numGoodSentiments, numBadSentiments>

        FileInputFormat.addInputPath(this, new Path(batchInputPath));
        FileOutputFormat.setOutputPath(this, new Path(batchOutputPath));
        this.setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    public static void main(String args[]) throws InterruptedException, IOException, ClassNotFoundException {

        // create configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", Globals.hdfsURI);
        FileSystem fs = FileSystem.get(conf);

        // clear output dir
        if (fs.exists(new Path(Globals.batchOutputPath)))
            fs.delete(new Path(Globals.batchOutputPath), true);

        // create and start Batch Layer
        Job batchLayer = new BatchLayer(conf, "BatchTwitterSentimentAnalysis", Globals.batchInputPath, Globals.batchOutputPath);
        batchLayer.setJarByClass(BatchLayer.class);
        batchLayer.waitForCompletion(true);
    }
}
