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

            // some random write, TODO: remove
            context.write(new Text("ciao"), new IntWritable((int)(Math.random()*2)));
            context.write(new Text("hello"), new IntWritable((int)(Math.random()*2)));
            context.write(new Text("world"), new IntWritable((int)(Math.random()*2)));

            // TODO implement
//            String timestampKey = ...;
//            per ogni tweet nel csv {
//                int sentiment = getSentiment(tweet);
//                context.write(timestampKey, sentiment);
//            }
        }

//    public static boolean evalTweet(String text, String query){
//       //TODO use the classificator ?!
//
//    }



}
