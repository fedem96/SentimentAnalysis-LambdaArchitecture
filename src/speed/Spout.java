package speed;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class Spout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp","sentiment"));
    }


    @Override
    public void nextTuple() {
        //TODO see how get the data from file
        String timestamp;
        String sentiment;
        collector.emit(new Values(timestamp,sentiment));
    }
}