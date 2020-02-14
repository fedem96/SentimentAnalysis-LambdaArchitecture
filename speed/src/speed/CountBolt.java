package speed;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    private String inProgressTimestamp;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        negative = 0;
        positive = 0;

        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", Globals.hdfsURI);
        fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Path hdfsreadpath = new Path(Globals.syncProgressTimestamp); //Create a path
        FSDataInputStream is = null; //Init input stream
        try {
            is = fs.open(hdfsreadpath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            inProgressTimestamp= IOUtils.toString(is, "UTF-16");
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

        //Create a path
        Path hdfswritepath = new Path(Globals.speedOutputPath + "/" + key + ".txt");
        //Init output stream
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(hdfswritepath);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        //Classical output stream usage
        String output = positive + "," + negative + "," + currentTimestamp;

        try {
            outputStream.writeChars(output);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

    }
}
