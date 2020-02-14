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
import java.util.Map;

//TODO bolt
class ClassifierBolt extends BaseBasicBolt {

    private Classifier classifier;

    public void prepare(Map conf, TopologyContext context){
        try {
            classifier = new Classifier("dataset/classifier_weights.lpc");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "sentiment", "timestamp"));
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {

        String[] values = tuple.getString(0).split(",", 2);

        // select output key from timestamp
        String tweetTimestamp = values[0];
        if(tweetTimestamp.length() < 21){
            System.err.println("Timestamp too short");
            return;
        }
        String key = Globals.timestampToKey(tweetTimestamp);

        // classify tweet
        String tweet = values[1];
        int sentiment;
        if(classifier.evaluateTweet(tweet).equals("neg")){
            sentiment = 0;
        }else{
            sentiment = 4;
        }

        // send result to CountBolt
        collector.emit(new Values(key, sentiment, tweetTimestamp));

        System.out.println("BOLT CLASSIFIER timestamp cur: " + tweetTimestamp + ", key: "+ key + ", sentiment: " + sentiment);
    }


}

