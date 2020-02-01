package batch;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


//TODO see how use hbase ?!
public class Map extends Mapper<Object, Text, Text, IntWritable> {

//    //TODO INIZIALIZE
//    Map(classify c){
//        //TODO declare the classification model
//        //classify c = new classify(classifierPath);
//    }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String content = value.toString();
            System.out.println("content: " + content);
            // TODO implement
        }

//    public static boolean evalTweet(String text, String query){
//       //TODO use the classificator ?!
//
//    }



}
