package batch;


import classify.Classifier;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import utils.Globals;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;


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
        //FIXME
//        if(values[0].length() < 33){
//            System.err.println("Timestamp too short");
//            return;
//        }
        String timestamp = values[0].substring(0,10);
        // change timestamp format
        SimpleDateFormat dateFormat = new SimpleDateFormat("MMM dd yyyy", Locale.ENGLISH);
        SimpleDateFormat newDateFormat = new SimpleDateFormat(Globals.datePattern, Locale.ENGLISH);

        try {
            timestamp = newDateFormat.format(dateFormat.parse(timestamp));
        } catch (ParseException e) {
            e.printStackTrace();
            return;
        }

        String tweet = values[1];

        // args[*****]
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

        context.write(new Text(timestamp), new IntWritable(sentiment));


    }

}
