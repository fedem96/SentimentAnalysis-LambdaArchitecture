package speed;

import classify.Classifier;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

//TODO bolt
class Bolt extends BaseBasicBolt {

    Classifier classifier;

    public void prepare(Map conf, TopologyContext context){
        try {
            classifier = new Classifier("/home/iacopo/Scrivania/Sentiment/dataset/classifier_weights.lpc");
        }catch(Exception ex){
            System.out.println("Problem with classificator file");
            System.out.println(ex);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "sentiment"));
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        /*
        collector.emit( new Values(contribution.timestamp,
                        contribution.sentiment ));
                        //TODO classifier.evaluateTweet(tuple.getString(0)) put in contibution.sentiment
        */
        //TODO bolt need to classify and create a file whit timestamp,sentiment
        //collector.emit(new Values(classifier.evaluateTweet(tuple.getString(0))));
        System.out.println(tuple.getString(0));
    }


}

