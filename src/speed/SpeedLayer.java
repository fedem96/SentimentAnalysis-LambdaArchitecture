package speed;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.lang.module.Configuration;

public class SpeedLayer {

        LocalCluster cluster;

        public SpeedLayer(String jobName, String batchInputPath, String speedInputPath, String speedOutputPath) throws Exception {

            TopologyBuilder builder = new TopologyBuilder();

            //FIXME we have this data in constructor
            Spout textReaderSpout = new Spout(batchInputPath,speedInputPath,speedOutputPath);

            // FIXME File in input are processed maybe not all ?

            builder.setSpout("spuot", textReaderSpout, 4);

            builder.setBolt("bolt", new Bolt(), 4).shuffleGrouping("bolt");

            //TODO see next
            LocalCluster cluster = new LocalCluster();

            Config conf = new Config();
            //FIXME
            conf.put("classifier_path", args[0]);

            cluster.submitTopology("tweet-sentiment", conf, builder.createTopology());

            //Thread.sleep(10000);
            //cluster.shutdown();
        }

        public void shutdownCluster(){
            this.cluster.shutdown();
        }

}
