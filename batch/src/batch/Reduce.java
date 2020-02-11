package batch;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int numGoodSentiments = 0;
        int numBadSentiments = 0;
        int total=0;
        for (IntWritable val : values) {
            int v = val.get();
            if(v == 4)
                numGoodSentiments++;
            else if(v == 0)
                numBadSentiments++;
            total++;
        }
//        context.write(key, new IntPair(numGoodSentiments, numBadSentiments));
        context.write(key, new Text(numGoodSentiments + "," + numBadSentiments));
        System.out.println("Reduce: key="+key+", good:"+numGoodSentiments+", bad:"+numBadSentiments+", total:" + total);
    }

}