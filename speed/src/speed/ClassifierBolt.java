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
class ClassifierBolt extends BaseBasicBolt {

    Classifier classifier;
    private String value;

    public void prepare(Map conf, TopologyContext context){
        value = (String) conf.get("timestamp");
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "sentiment"));// key, new Text(numGoodSentiments + "," + numBadSentiments)
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple);
        int numGoodSentiments = 0;
        int numBadSentiments = 0;
        int total=0;
        //FIXME precess one tuple per time?
//        for (Tuple t : tuple) {
//            int v = t.getInteger(1);
//            if(v == 4)
//                numGoodSentiments++;
//            else if(v == 0)
//                numBadSentiments++;
//            total++;
//        }
        String timestamp = tuple.getString(0);
        String sentiment = new String(numGoodSentiments + "," + numBadSentiments);

        collector.emit( new Values(timestamp, sentiment));

        System.out.println("BOLT  good:"+numGoodSentiments+", bad:"+numBadSentiments+", total:" + total);
    }


}

