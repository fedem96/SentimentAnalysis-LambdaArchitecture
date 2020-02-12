package speed;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import utils.Globals;

import java.io.IOException;
import java.util.Map;

public class CountBolt extends BaseRichBolt {
    private int positive;
    private int negative;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        negative = 0;
        positive = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        // System.out.println(tuple);
        String timestamp = tuple.getString(0);
        int sentiment = tuple.getInteger(1);

        if (sentiment == 4) {
            positive++;
        }else {
            negative++;
        }

        System.out.println("BOLT COUNT timestamp: " + timestamp + ", positive: " + positive + ", negative: " + negative);


        // =====================================
        Configuration conf = new Configuration();
        // Set FileSystem URI
        conf.set("fs.defaultFS", Globals.hdfsURI);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Create a path
        Path hdfswritepath = new Path(Globals.speedOutputPath + "/" + timestamp + ".txt");
        //Init output stream
        FSDataOutputStream outputStream = null;
        try {
            outputStream = fs.create(hdfswritepath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //Cassical output stream usage
        String output = positive + "," + negative;

        try {
            outputStream.writeBytes(String.valueOf(output.getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //logger.info("End Write file into hdfs");

        /*
        File file = new File( "/home/iacopo/Scrivania/FILES/" + timestamp + ".txt");
        // if file doesnt exists, then create it
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        FileWriter fw = null;
        try {
            fw = new FileWriter(file.getAbsoluteFile());
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedWriter bw = new BufferedWriter(fw);
        try {
            bw.write(positive + "," + negative);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        */

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
