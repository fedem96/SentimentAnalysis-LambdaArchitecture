package speed;

import classify.Classifier;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.Map;

//TODO bolt
class ClassifierBolt extends BaseBasicBolt {

    Classifier classifier;
    private String[] values;

    public void prepare(Map conf, TopologyContext context){
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "key", "sentiment"));// key, sentiment
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // System.out.println(tuple);
        values = tuple.getString(0).split(",",2);
        //select the timestamp without time
        String timeStampCurr = values[0];
        String key = values[0].substring(0,10);
        // change timestamp format
//        SimpleDateFormat dateFormat = new SimpleDateFormat("MMM dd yyyy", Locale.ENGLISH);
//        SimpleDateFormat newDateFormat = new SimpleDateFormat(Globals.datePattern, Locale.ENGLISH);
//
//        try {
//            key = newDateFormat.format(dateFormat.parse(key));
//        } catch (ParseException e) {
//            e.printStackTrace();
//            return;
//        }

        String tweet = values[1];

        try {
            classifier = new Classifier("dataset/classifier_weights.lpc");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        } catch (IOException e) {
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

        collector.emit( new Values(timeStampCurr, key, sentiment));

        System.out.println("BOLT CLASSIFIER timestamp cur: " + timeStampCurr + ", key: "+ key + ", sentiment: " + sentiment);
    }


}

