package speed;

import classify.Classifier;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import utils.Globals;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Map;

//TODO bolt
class ClassifierBolt extends BaseBasicBolt {

    Classifier classifier;
    private String[] values;

    public void prepare(Map conf, TopologyContext context){
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "sentiment"));// key, sentiment
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // System.out.println(tuple);
        values = tuple.getString(0).split(",",2);
        //select the timestamp without time
        String timestamp = values[0].substring(4,10) + values[0].substring(23,28);
        // change timestamp format
        SimpleDateFormat dateFormat = new SimpleDateFormat("MMM dd yyyy", Locale.ENGLISH);
        SimpleDateFormat newDateFormat = new SimpleDateFormat(Globals.datePattern, Locale.ENGLISH);

        try {
            timestamp = newDateFormat.format(dateFormat.parse(timestamp));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String tweet = values[1];

        try {
            //FIXME set path with args?!
            classifier = new Classifier("/home/iacopo/Scrivania/Sentiment/dataset/classifier_weights.lpc");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        int sentiment;
        // System.out.println(classifier.evaluateTweet(tweet));
        if(classifier.evaluateTweet(tweet).equals("neg")){
            sentiment = 0;
        }else{
            sentiment = 4;
        }

        collector.emit( new Values(timestamp, sentiment));

        System.out.println("BOLT CLASSIFIER: "+ timestamp + ", sentiment: " + sentiment);
    }


}

