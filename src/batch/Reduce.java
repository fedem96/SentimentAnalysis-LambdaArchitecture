package batch;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import types.IntPair;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable, Text, IntPair> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int numGoodSentiments = 0;
        int numBadSentiments = 0;
        for (IntWritable val : values) {
            int v = val.get();
            if(v == 4)
                numGoodSentiments++;
            else if(v == 0)
                numBadSentiments++;
        }
        context.write(key, new IntPair(numGoodSentiments, numBadSentiments));
    }

}