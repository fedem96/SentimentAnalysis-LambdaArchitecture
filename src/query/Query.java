package query;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import utils.Globals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Query {

    public static void main(String args[]) throws IOException {
        if(args.length == 0){
            System.err.println("Error: not enough arguments");
            return;
        }

        // create configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", Globals.hdfsURI);
        FileSystem fs = FileSystem.get(conf);

        if(args.length == 1){ // specific date
            String date = args[0];

            FSDataInputStream inputStream = fs.open(new Path(Globals.batchOutputFile));
            BufferedReader br=new BufferedReader(new InputStreamReader(inputStream));
            while (br.ready()){
                String line = br.readLine();
                System.out.println(line);
            }

            String out = IOUtils.toString(inputStream, "UTF-8");
            inputStream.close();
            fs.close();
//            System.out.println(out);

        } else { // dates interval
            String beginDate = args[0];
            String endDate = args[1];
        }
    }

}
