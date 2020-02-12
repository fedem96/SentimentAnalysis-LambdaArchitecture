package speed;

import classify.Classifier;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;

import java.util.Map;

public class Spout extends HdfsSpout {

    /*          .withOutputFields("line")
                .setHdfsUri(Globals.hdfsURI)  // required
                .setSourceDir(Globals.speedInputPath)          // required
                .setArchiveDir(Globals.speedOutputPath)
     */
    private SpoutOutputCollector collector;
    private Classifier classifier;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
        System.out.println("OPEN SPOUT");
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp","sentiment"));
    }


    @Override
    public void nextTuple() {
        System.out.println("NEXT TUPLE");
//        try {
//            classifier = new Classifier("/home/iacopo/Scrivania/Sentiment/dataset/classifier_weights.lpc");
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//
//        //TODO see how get the data from file
//        String values[] = collector.toString().split(",",2);
//        System.out.println(values.toString());
//        /*--------------*/
//        String timestamp = values[0].substring(0,12) + values[0].substring(25,30);
//        String tweet = values[1];
//
//        int sentiment;
//        if(classifier.evaluateTweet(tweet).equals("neg")){
//            sentiment = 0;
//        }else{
//            sentiment = 4;
//        }
//
//        collector.emit(new Values(timestamp,sentiment));
    }
}