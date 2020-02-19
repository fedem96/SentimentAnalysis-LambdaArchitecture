package query;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import utils.Globals;

import java.io.IOException;
import java.util.*;

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

                HashMap<String, int[]> batchNums = queryBatch(queryDate, queryDate, fs, conf);

                HashMap<String, int[]> speedNums = querySpeed(queryDate, queryDate, fs);

                Set<String> words = batchNums.keySet();
                words.addAll(speedNums.keySet());

                for(String word: words) {
                    int numGood = batchNums.get(word)[0] + speedNums.get(word)[0];
                    int numBad = batchNums.get(word)[1] + speedNums.get(word)[1];
                    System.out.println(word);
                    System.out.println("Num good tweets on " + queryDate + ": " + numGood + " (" + batchNums.get(word)[0] + " from batch, " + speedNums.get(word)[0] + " from speed)");
                    System.out.println("Num bad tweets on " + queryDate + ": " + numBad + " (" + batchNums.get(word)[1] + " from batch, " + speedNums.get(word)[1] + " from speed)");
                }
                System.out.println();

                Thread.sleep(SLEEP_TIME);
            }

        } else { // dates interval
            String beginDate = args[0];
            String endDate = args[1];

            while (true) {

                HashMap<String, int[]> batchNums = queryBatch(beginDate, endDate, fs, conf);

                HashMap<String, int[]> speedNums = querySpeed(beginDate, endDate, fs);

                Set<String> words = new HashSet<>();
                words.addAll(batchNums.keySet());
                words.addAll(speedNums.keySet());

                for(String word: words) {
                    int numGood = batchNums.get(word)[0] + speedNums.get(word)[0];
                    int numBad = batchNums.get(word)[1] + speedNums.get(word)[1];
                    System.out.println(word);
                    System.out.println("Num good tweets between " + beginDate + " and " + endDate + ": " + numGood + " (" + batchNums.get(word)[0] + " from batch, " + speedNums.get(word)[0] + " from speed)");
                    System.out.println("Num bad tweets between " + beginDate + " and " + endDate + ": " + numBad + " (" + batchNums.get(word)[1] + " from batch, " + speedNums.get(word)[1] + " from speed)");
                }
                System.out.println();

                Thread.sleep(SLEEP_TIME);
            }
        }

    }

    static HashMap<String, int[]> queryBatch(String beginDate, String endDate, FileSystem fs, Configuration conf) throws IOException {
        Text key = new Text();
        Text val = new Text();
        HashMap<String, int[]> hm = new HashMap<>();
        hm.put("total", new int[2]);
        for(String word: Globals.keywords)
            hm.put(word, new int[2]);

        String outputPath;
        try {
            outputPath = Globals.readStringFromHdfsFile(fs, Globals.syncLastBatchOutput);
        }
        catch (IOException ioe){
            return hm;
        }

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
                String values[] = key.toString().split("/");
                String tweetDate = values[0];
                String keyword = values[1];
                if(beginDate.compareTo(tweetDate) <= 0 && tweetDate.compareTo(endDate) <= 0){
                    String counts[] = val.toString().split(",");
                    hm.get(keyword)[0] += Integer.parseInt(counts[0]);
                    hm.get(keyword)[1] += Integer.parseInt(counts[1]);
                }
            }
            reader.close();
        }

        return hm;
    }

    static HashMap<String, int[]> querySpeed(String beginDate, String endDate, FileSystem fs) throws IOException {

        HashMap<String, int[]> hm = new HashMap<>();
        hm.put("total", new int[2]);
        for(String word: Globals.keywords)
            hm.put(word, new int[2]);

        String processedTimestamp = Globals.readStringFromHdfsFile(fs, Globals.syncProcessedTimestamp);
        String inProgressTimestamp = Globals.readStringFromHdfsFile(fs, Globals.syncProgressTimestamp);

        List<String> timestamps = new ArrayList<String>();
        timestamps.add(processedTimestamp);
        if(!processedTimestamp.equals(inProgressTimestamp))
            timestamps.add(inProgressTimestamp);

        for(String timestamp: timestamps) {
            Path path = new Path(Globals.speedOutputPath + "/" + timestamp);
            if (!fs.exists(path))
                continue;
            System.out.println("reading speed: " + timestamp);

            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(path, true);
            while (fileStatusListIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusListIterator.next();
                String keyword = fileStatus.getPath().getName().replace(".txt", "");
                String tweetDate = fileStatus.getPath().getParent().getName();
                int good=0, bad=0;
                if (beginDate.compareTo(tweetDate) <= 0 && tweetDate.compareTo(endDate) <= 0) {
                    FSDataInputStream in = fs.open(fileStatus.getPath());
                    String line = IOUtils.toString(in, "UTF-16");
                    in.close();
                    try {
                        String counts[] = line.split(",");
                        if (!counts[0].equals("")) {
                            good = Integer.parseInt(counts[0]);
                            bad = Integer.parseInt(counts[1]);
                            hm.get(keyword)[0] += good;
                            hm.get(keyword)[1] += bad;
                        }
                    } catch (NumberFormatException nfe) {
                        nfe.printStackTrace();
                    }
                }

                System.out.println(keyword);
                System.out.println("good: " + good);
                System.out.println("bad: " + bad);
            }
        }

        return hm;
    }

}
