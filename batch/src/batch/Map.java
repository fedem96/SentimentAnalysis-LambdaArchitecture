package batch;


import classify.Classifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Globals;

import java.io.IOException;

public class Map extends Mapper<Object, Text, Text, IntWritable> {

    private Classifier classifier;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("MAPPER - key: " + key.toString() + ", value: "+ value.toString());

        String values[] = value.toString().split(",",2);
        if(values.length != 2){
            System.err.println("Invalid line");
            return;
        }

        String tweetTimestamp = values[0];
        if(tweetTimestamp.length() < 21){
            System.err.println("Timestamp too short");
            return;
        }

        String tweet = values[1];

        try {
            classifier = new Classifier("dataset/classifier_weights.lpc");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
        int sentiment;
        // System.out.println(classifier.evaluateTweet(tweet));
        if(classifier.evaluateTweet(tweet).equals("neg")){
            sentiment = 0;
        }else{
            sentiment = 4;
        }


        String[] words = tweet.toLowerCase().split(" ");
        String outputKey;

        for(String word: words) {

            if (!Globals.keywords.contains(word)) {
                continue;
            }

            outputKey = Globals.timestampToKey(tweetTimestamp) + "/" + word;

            context.write(new Text(outputKey), new IntWritable(sentiment));
        }

        outputKey = Globals.timestampToKey(tweetTimestamp) + "/total";
        context.write(new Text(outputKey), new IntWritable(sentiment));


    }

}
