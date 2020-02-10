package speed;

import classify.Classifier;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

//TODO bolt
class Bolt extends BaseBasicBolt {

    Classifier classifier;

    public void prepare(Map conf, TopologyContext context){
        //FIXME use args
        String abspath = (String) conf.get("/home/iacopo/Scrivania/Sentiment/dataset/classifier_weights.lpc");
        try {
            classifier = new Classifier(abspath);
        }catch(Exception ex){
            System.out.println("Problem with classificator file");
            System.out.println(ex);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentiment"));
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        collector.emit(new Values(classifier.evaluateTweet(tuple.getString(0))));
        System.out.println(tuple.getString(0));
    }


}