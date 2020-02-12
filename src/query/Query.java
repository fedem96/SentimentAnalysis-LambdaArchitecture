package query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import utils.Globals;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class Query {

    public static void main(String args[]) throws IOException, ParseException {
        if(args.length == 0){
            System.err.println("Error: not enough arguments");
            return;
        }

        // create configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", Globals.hdfsURI);
        FileSystem fs = FileSystem.get(conf);


        SimpleDateFormat dateFormat = new SimpleDateFormat(Globals.datePattern, Locale.ENGLISH);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(Globals.batchOutputFile)));
        Text key = new Text();
        Text val = new Text();

        if(args.length == 1){ // specific date
            Date queryDate = dateFormat.parse(args[0]);

            while (reader.next(key, val)) {
                Date tweetDate = dateFormat.parse(key.toString());
                if(tweetDate.compareTo(queryDate) == 0){
                    String counts[] = val.toString().split(",");
                    String numGood = counts[0];
                    String numBad = counts[1];
                    System.out.println("Num good tweets on " + dateFormat.format(queryDate) + ": " + numGood);
                    System.out.println("Num bad tweets on " + dateFormat.format(queryDate) + ": " + numBad);
                    break;
                }
            }


        } else { // dates interval
            Date beginDate = dateFormat.parse(args[0]);
            Date endDate = dateFormat.parse(args[1]);

            int numGood = 0;
            int numBad = 0;
            while (reader.next(key, val)) {
                Date tweetDate = dateFormat.parse(key.toString());
                if(!beginDate.after(tweetDate) && !endDate.before(tweetDate)){
                    String counts[] = val.toString().split(",");
                    numGood += Integer.parseInt(counts[0]);
                    numBad += Integer.parseInt(counts[1]);
                }
            }

            System.out.println("Num good tweets between " + dateFormat.format(beginDate) + " and " + dateFormat.format(endDate)  + " (included): " + numGood);
            System.out.println("Num bad tweets between " + dateFormat.format(beginDate) + " and " + dateFormat.format(endDate)  + " (included): " + numBad);
        }

        reader.close();
    }

}
