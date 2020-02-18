package query;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import utils.Globals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Query {


    public static void main(String args[]) throws IOException, InterruptedException {
        if(args.length == 0){
            System.err.println("Error: not enough arguments");
            return;
        }

        final long SLEEP_TIME = 3000; // milliseconds

        // create configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", Globals.hdfsURI);
        FileSystem fs = FileSystem.get(conf);

        if(args.length == 1){ // specific date
            String queryDate = args[0];

            while (true){

                int[] batchNums = queryBatch(queryDate, queryDate, fs, conf);
                int numGood = batchNums[0];
                int numBad = batchNums[1];

                int[] speedNums = querySpeed(queryDate, queryDate, fs);
                numGood += speedNums[0];
                numBad += speedNums[1];

                System.out.println("Num good tweets on " + queryDate + ": " + numGood + " (" + batchNums[0] +" from batch, " + speedNums[0] + " from speed)");
                System.out.println("Num bad tweets on " + queryDate + ": " + numBad + " (" + batchNums[1] +" from batch, " + speedNums[1] + " from speed)");
                System.out.println();

                Thread.sleep(SLEEP_TIME);
            }

        } else { // dates interval
            String beginDate = args[0];
            String endDate = args[1];

            while (true) {

                int batchNums[] = queryBatch(beginDate, endDate, fs, conf);
                int numGood = batchNums[0];
                int numBad = batchNums[1];

                int[] speedNums = querySpeed(beginDate, endDate, fs);
                numGood += speedNums[0];
                numBad += speedNums[1];

                System.out.println("Num good tweets between " + beginDate + " and " + endDate + " (included): " + numGood + " (" + batchNums[0] + " from batch, " + speedNums[0] + " from speed)");
                System.out.println("Num bad tweets between " + beginDate + " and " + endDate + " (included): " + numBad + " (" + batchNums[1] + " from batch, " + speedNums[1] + " from speed)");
                System.out.println();

                Thread.sleep(SLEEP_TIME);
            }
        }

    }

    private static int[] queryBatch(String beginDate, String endDate, FileSystem fs, Configuration conf) throws IOException {
        Text key = new Text();
        Text val = new Text();
        int[] nums = new int[2];

        String outputPath = Globals.readStringFromHdfsFile(fs, Globals.syncLastBatchOutput);
        System.out.println("reading batch: " + outputPath);
        RemoteIterator<LocatedFileStatus> filesIterator = fs.listFiles(new Path(outputPath), false);
        while (filesIterator.hasNext()) {
            LocatedFileStatus file = filesIterator.next();
            if(!file.isFile())
                continue;
            Path filePath = file.getPath();
            if(!filePath.getName().startsWith("part-"))
                continue;

            SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(filePath));
            while (reader.next(key, val)) {
                String tweetDate = key.toString();
                if(beginDate.compareTo(tweetDate) <= 0 && tweetDate.compareTo(endDate) <= 0){
                    String counts[] = val.toString().split(",");
                    nums[0] += Integer.parseInt(counts[0]);
                    nums[1] += Integer.parseInt(counts[1]);
                }
            }
            reader.close();
        }



        return nums;
    }

    public static int[] querySpeed(String beginDate, String endDate, FileSystem fs) throws IOException {

        int[] nums = new int[2];
        int numGood = 0;
        int numBad = 0;

        String processedTimestamp = Globals.readStringFromHdfsFile(fs, Globals.syncProcessedTimestamp);
        String inProgressTimestamp = Globals.readStringFromHdfsFile(fs, Globals.syncProgressTimestamp);

        List<String> timestamps = new ArrayList<String>();
        timestamps.add(processedTimestamp);
        if(!processedTimestamp.equals(inProgressTimestamp))
            timestamps.add(inProgressTimestamp);

        int pg = 0, pb = 0;

        for(String timestamp: timestamps) {
            Path path = new Path(Globals.speedOutputPath + "/" + timestamp);
            if (!fs.exists(path))
                continue;
            System.out.println("reading speed: " + timestamp);
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, false);

            while (fileStatusListIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusListIterator.next();
                String tweetDate = fileStatus.getPath().getName().replace(".txt", "");
                if(beginDate.compareTo(tweetDate) <= 0 && tweetDate.compareTo(endDate) <= 0){
                    FSDataInputStream in = fs.open(fileStatus.getPath());
                    String line = IOUtils.toString(in, "UTF-16");
                    in.close();
                    try {
                        String counts[] = line.split(",");
                        numGood += Integer.parseInt(counts[0]);
                        numBad += Integer.parseInt(counts[1]);
                    }
                    catch (NumberFormatException nfe){
                        nfe.printStackTrace();
                    }
                }
            }
            System.out.println("good: " + (numGood-pg));
            System.out.println("bad: " + (numBad-pb));
            pg = numGood;
            pb = numBad;
        }

        nums[0] = numGood;
        nums[1] = numBad;

        return nums;
    }

}
