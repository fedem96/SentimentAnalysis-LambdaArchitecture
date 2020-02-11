package speed;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class SpeedLayer {


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        Spout fileReaderSpout = new Spout();

        /* File that doesn't have .ignore extension will be processed */

        builder.setSpout("fileReaderSpout", fileReaderSpout, 4);


        builder.setBolt("bolt", new Bolt(), 4).fieldsGrouping("fileReaderSpout", new Fields("timestamp"));

        LocalCluster cluster = new LocalCluster();

        Config conf = new Config();
        conf.put("classifier_path", args[0]);
        if(args.length <= 0){ // classifier path
            System.err.println("Error: not enough args");
            return;
        }

        cluster.submitTopology("tweet-sentiment", conf, builder.createTopology());

        Thread.sleep(10000);
        cluster.shutdown();
    }

}
