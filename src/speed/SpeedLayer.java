package speed;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.lang.module.Configuration;

public class SpeedLayer {

    LocalCluster cluster;
    String jobName;
    String batchInputPath;
    String speedInputPath;
    String speedOutputPath;
        

    public SpeedLayer(LocalCluster cluster, String jobName, String batchInputPath, String speedInputPath, String speedOutputPath) throws Exception {
        this.cluster = cluster;
        this.jobName = jobName;
        this.batchInputPath = batchInputPath;
        this.speedInputPath = speedInputPath;
        this.speedOutputPath = speedOutputPath;
        //TODO
        main();
    }

    public void shutdownCluster(){
            this.cluster.shutdown();
    }

    //FIXME la mia idea è richiamare nel main del programma il costruttore speed layer che a sua volta lancia il main di questa classe e parte all'infinito possibile?
    public static void main() throws Exception {
        //FIXME here use this.blablabla come parametri
        
        TopologyBuilder builder = new TopologyBuilder();
        
        //FIXME we have this data in constructor
        Spout fileReaderSpout = new Spout(this.batchInputPath, this.speedInputPath, this.speedOutputPath);

        // FIXME File in input are processed maybe not all ?

        builder.setSpout("spuot", fileReaderSpout, 4);

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
}
