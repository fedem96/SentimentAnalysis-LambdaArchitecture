package speed;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.hdfs.spout.HdfsSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import utils.Globals;

public class SpeedLayer {


    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        //HdfsSpout fileReaderSpout = new Spout().withOutputFields("timestamp","sentiment").setHdfsUri(Globals.hdfsURI).setSourceDir(Globals.speedInputPath).setArchiveDir(Globals.speedOutputPath).setReaderType("org.apache.storm.hdfs.spout.TextFileReader");

        /*          .withOutputFields("line")
                .setHdfsUri(Globals.hdfsURI)  // required
                .setSourceDir(Globals.speedInputPath)          // required
                .setArchiveDir(Globals.speedOutputPath)
        */

        /* File that doesn't have .ignore extension will be processed */

        HdfsSpout fileReaderSpout = new HdfsSpout().setReaderType("text")
                .withOutputFields("line")
                .setHdfsUri(Globals.hdfsURI)  // reqd
                .setSourceDir(Globals.speedInputPath)              // reqd
                .setArchiveDir(Globals.speedOutputPath).setBadFilesDir(Globals.badFiles);     // required

        // read the hdfs file and pass to the classifier bolt
        builder.setSpout("fileReaderSpout", fileReaderSpout, 4);

        // get the "line" from tuple.get(0).toString() --> split and classify tweet
        builder.setBolt("ClassifierBolt", new ClassifierBolt(), 4).shuffleGrouping("fileReaderSpout");

        // group by timestamp the tweet and count the sentiment
        builder.setBolt("CountBolt", new CountBolt(), 4).fieldsGrouping("ClassifierBolt", new Fields("timestamp"));

        LocalCluster cluster = new LocalCluster();

        Config conf = new Config();
        conf.put("classifier_path", args[0]);

        if(args.length <= 0){ // classifier path
            System.err.println("Error: not enough args");
            return;
        }

        cluster.submitTopology("tweet-sentiment", conf, builder.createTopology());

        // Thread.sleep(10000);
        // cluster.shutdown();
    }

}