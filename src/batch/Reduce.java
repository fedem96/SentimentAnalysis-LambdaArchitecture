package batch;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import types.IntPair;

public class Reduce extends Reducer<Text, IntWritable, Text, IntPair> {


//    @Override
//    protected void setup(){
//
//    }
//
//    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//        //TODO make a list <key,value> eg. <TimeStamp, 0: #0, 1: #1> day/hour ?
//
//
//        //TODO save the list on hash base
//        save();
//
//    }
//
//    @Override
//    protected void save(){
//
//    }

}