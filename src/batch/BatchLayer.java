package batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import types.IntPair;

import java.io.IOException;

public class BatchLayer extends Job {

//    //TODO here make the executable that uses map and reduce
//    public int run(String[] args) throws Exception {
//
//
//        boolean success = job.waitForCompletion(true);
//        return success ? 0 : 1;
//    }
//
//
//    public static void main(String[] args) throws Exception {
//        int res = ToolRunner.run(new Configuration(), new Map(), args);
//        System.exit(res);
//    }


    public BatchLayer(Configuration conf, String jobName, String batchInputPath, String batchOutputPath) throws IOException {
        super(conf, jobName);

        this.setMapperClass(Map.class);
        this.setReducerClass(Reduce.class);

        this.setMapOutputKeyClass(Text.class);           // timestamp
        this.setMapOutputValueClass(IntWritable.class);  // sentiment

        this.setOutputKeyClass(Text.class);              // timestamp
        this.setOutputValueClass(IntPair.class);         // <numGoodSentiments, numBadSentiments>

        FileInputFormat.addInputPath(this, new Path(batchInputPath));
        FileOutputFormat.setOutputPath(this, new Path(batchOutputPath));
        this.setOutputFormatClass(SequenceFileOutputFormat.class);
    }
}
