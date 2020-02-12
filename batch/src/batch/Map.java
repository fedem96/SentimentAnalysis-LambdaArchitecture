package batch;


import classify.Classifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


//TODO see how use hbase ?!
public class Map extends Mapper<Object, Text, Text, IntWritable> {

    private Classifier classifier;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // String content = value.toString();
        // System.out.println("content: " + content);
        // System.out.println(context.toString());
        System.out.println("MAPPER, key " + key.toString() + "value "+ value.toString());

        String values[] = value.toString().split(",",2);
        if(values.length != 2){
            System.err.println("Invalid line");
            return;
        }
        if(values[0].length() < 25){
            System.err.println("Timestamp too short");
            return;
        }
        String timestamp = values[0].substring(0,12) + values[0].substring(25,30);
        //FIXME chose the correct timestamp
        String tweet = values[1];

        // args[*****]
        try {
            classifier = new Classifier("dataset/classifier_weights.lpc");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        int sentiment;
        // System.out.println(classifier.evaluateTweet(tweet));
        if(classifier.evaluateTweet(tweet).equals("neg")){
            sentiment = 0;
        }else{
            sentiment = 4;
        }

        context.write(new Text(timestamp), new IntWritable(sentiment));


    }

}
