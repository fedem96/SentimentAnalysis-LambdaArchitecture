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
    private String inProgressTimestamp;

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
            inProgressTimestamp = Globals.readStringFromHdfsFile(fs, Globals.syncProgressTimestamp);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        // here i have the in progress timestamp and the current timestamp
        String currentTimestamp = tuple.getString(0);
        String key = tuple.getString(1);
        int sentiment = tuple.getInteger(2);

        if (sentiment == 4) {
            positive++;
        }else {
            negative++;
        }

        System.out.println("BOLT COUNT key: " + key + ", positive: " + positive + ", negative: " + negative);
        //Classical output stream usage
        String output = positive + "," + negative + "," + currentTimestamp;
        try {
            String inProgressTimestamp = Globals.readStringFromHdfsFile(fs, Globals.syncProgressTimestamp);
            if(!previousTimestamp.equals(inProgressTimestamp)){
                negative = 0;
                positive = 0;
            }
            previousTimestamp = inProgressTimestamp;
            Globals.writeStringToHdfsFile(fs, output, Globals.speedOutputPath + "/" + inProgressTimestamp + "/"+ key + ".txt");
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
