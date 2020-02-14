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
import java.sql.Timestamp;
import java.util.Date;

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

        int outputIndex = 0;

        while (true) {
            // select output path
            String outputPath = Globals.batchOutputPaths[outputIndex];
            outputIndex = (outputIndex + 1) % Globals.batchOutputPaths.length;

            // clear output dir
            if (fs.exists(new Path(outputPath)))
                fs.delete(new Path(outputPath), true);

            // get timestamp and save
            Date date= new Date();
            long time = date.getTime(); // current time in milliseconds
            String ts = new Timestamp(time).toString(); // create timestamp from millis


            // create and start Batch Layer
            Job batchLayer = new BatchLayer(conf, "BatchTwitterSentimentAnalysis", Globals.batchInputPath, outputPath);
            batchLayer.setJarByClass(BatchLayer.class);
            batchLayer.waitForCompletion(true);


            // write last folder


            // wait 5 seconds
            Thread.sleep(5000);
        }
    }

}
