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
        //String content = value.toString();
        //System.out.println("content: " + content);

        String values[] = value.toString().split(",",2);
        String timestamp = values[0];
        //FIXME chose the correct timestamp
        timestamp = values[0].substring(0,12) + values[0].substring(25,30);
        String tweet = values[1];

        // args[*****]
        try {
            classifier = new Classifier("/home/iacopo/Scrivania/SentimentAnalysis-LambdaArchitecture/dataset/classifier_weights.lpc");
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
