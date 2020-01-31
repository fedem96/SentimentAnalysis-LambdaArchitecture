package BatchLayer;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import Classifier.Classifier;


//TODO see how use hbase ?!
public  class Map extends Mapper<Object, Text, LongWritable, Text> {

    //TODO INIZIALIZE
    Map(Classifier c){
        //TODO declare the classification model
        //Classifier c = new Classifier(classifierPath);
    }

    //TODO classify every tweet
    public void mapper(Object key, Text value, Context context) throws IOException, InterruptedException {
        /* Get a string (value), parse it in two pieces (ID and text)*/


    }

    public static boolean evalTweet(String text, String query){
       //TODO use the classificator ?!

    }


}
