package speed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import utils.Globals;

import java.io.IOException;
import java.util.Map;

public class CountBolt extends BaseBasicBolt {
    private int positive;
    private int negative;
    private FileSystem fs;
    private String previousTimestamp;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        negative = 0;
        positive = 0;

        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", Globals.hdfsURI);
        fs = null;
        previousTimestamp = "";
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        long start = System.currentTimeMillis();
        String key = tuple.getString(0);
        int sentiment = tuple.getInteger(1);
        String tweetTimestamp = tuple.getString(2);

        if (sentiment == 4) {
            positive++;
        }else {
            negative++;
        }

//        System.out.println("BOLT COUNT timestamp: " + tweetTimestamp + "key: " + key + ", positive: " + positive + ", negative: " + negative);
        try {
            // discard tweet if already processed by batch layer or if in progress (if in progress, it should have already been counted: if not, I can't count it anymore because the counters were reset)
            // TODO sistemare
//            String processedTimestamp = Globals.readStringFromHdfsFile(fs, Globals.syncProcessedTimestamp);

            String inProgressTimestamp = Globals.readStringFromHdfsFile(fs, Globals.syncProgressTimestamp);
            if(tweetTimestamp.compareTo(inProgressTimestamp) <= 0) {
                System.out.println("tweet skipped");
                return; // I want to count only tweets after inProgressTimestamp
            }

            String output = positive + "," + negative;
            if(!previousTimestamp.equals(inProgressTimestamp)){
                Globals.writeStringToHdfsFile(fs, output, Globals.speedOutputPath + "/" + inProgressTimestamp + "/" + key + ".txt");
                negative = 0;
                positive = 0;
                output = positive + "," + negative;
            }
            previousTimestamp = inProgressTimestamp;
            //fixme bug here inProgresTimestamp = ""
            if(Math.random() < 0.01)
                Globals.writeStringToHdfsFile(fs, output, Globals.speedOutputPath + "/" + inProgressTimestamp + "/" + key + ".txt");
            else
                System.out.println("skipped saving");

        } catch (IOException e) {
            e.printStackTrace();
        }
        long end = System.currentTimeMillis();
        System.out.println("COUNT time: " + (end-start));

    }
}
