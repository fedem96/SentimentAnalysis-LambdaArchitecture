package generate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import utils.Globals;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static java.lang.System.currentTimeMillis;



public class Generator extends Thread{

    private final String csvPath;

    private Sender toBatchSender;
    private Sender toSpeedSender;


    // PARAMETERS
    // deltaTime: milliseconds
    static int maxDeltaTimeBatch = 10 * 60 * 1000;
    static int maxDeltaTimeSpeed = 5 * 1024;
    // fileDim: number of bytes
    static int maxFileDimBatch = 10 * 60 * 1000;
    static int maxFileDimSpeed = 5 * 1024;


    public Generator(String hdfsURI, String csvPath, String batchInputPath, String speedInputPath) throws IOException {
        this.csvPath = csvPath;
        this.toBatchSender = new Sender(hdfsURI, batchInputPath, maxDeltaTimeBatch, maxFileDimBatch);
        this.toSpeedSender = new Sender(hdfsURI, speedInputPath, maxDeltaTimeSpeed, maxFileDimSpeed);

        System.out.println("Generator created");
    }

    private List<String[]> readCSV(String sep) throws IOException {
        System.out.println("Begin reading CSV");
        List<String[]> lines = new LinkedList<String[]>();

        BufferedReader csvReader = new BufferedReader(new FileReader(this.csvPath));
        String row;
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split(sep, 6);
            for(int i = 0; i < data.length; i++)
                data[i] = data[i].substring(1, data[i].length()-1);
            lines.add(data);
        }
        csvReader.close();
        System.out.println("CSV correctly read");
        return lines;
    }

    @Override
    public void run() {
        // read all tweets
        List<String[]> lines;
        try {
            lines = readCSV(",");
        } catch (IOException e) {
            System.err.println("Error while reading CSV input file");
            return;
        }
        // shuffle tweets
        System.out.println("Shuffle dataset");
        Collections.shuffle(lines);
        //INIZIALIZATION Script

        // create configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", Globals.hdfsURI);
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        // get timestamp and save
        String ts = Globals.currentTimestamp();
        try {
            Globals.writeStringToHdfsFile(fs, ts, Globals.syncProgressTimestamp);
            Globals.writeStringToHdfsFile(fs, ts, Globals.syncProcessedTimestamp);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // send tweets to Batch and Speed Layers
        try {
            try {
//            int count = 0;
                for (String[] line : lines) {
                    ts = Globals.currentTimestamp();
                    toBatchSender.send(ts, line[5]);
                    toSpeedSender.send(ts, line[5]);
                    // line[2]: timestamp of the tweet in the dataset
//                    toBatchSender.send(line[2], line[5]);
//                    toSpeedSender.send(line[2], line[5]);
//                    System.out.println("lines sent:" + count);
//                    count++;
//                    if(count == 20000)
//                        return;
                    Thread.sleep((long) (Math.random() * 3));
                }
                toBatchSender.flush();
                toSpeedSender.flush();
            } catch (IOException ioe) {
                System.err.println("Error: IOException");
            } catch (InterruptedException e) {
                System.err.println("Error: InterruptedException");
            } finally {
                toBatchSender.flush();
                toSpeedSender.flush();
            }
        }
        catch (IOException ioe){
            System.err.println("Error: IOException during flush");
        }

    }

    private class Sender {

        private long lastTime;
        private String hdfsURI;
        private String outputDir;
        private long maxDeltaTime;
        private long curFileDim;
        private long maxFileDim;
        private int numFile;
        private BufferedWriter bw;

        public Sender(String hdfsURI, String outputDir, long maxDeltaTime, long maxFileDim) throws IOException {
            this.hdfsURI = hdfsURI;
            this.outputDir = outputDir;
            this.maxDeltaTime = maxDeltaTime;
            this.maxFileDim = maxFileDim;
            lastTime = currentTimeMillis();
            curFileDim = 0;
            numFile = -1;
            bw = getNewBufferedWriter();

        }

        private BufferedWriter getNewBufferedWriter() throws IOException {
            numFile ++;
            Configuration hadoopConfig = new Configuration();
            hadoopConfig.set("fs.defaultFS", this.hdfsURI);
            FileSystem fileSystem = FileSystem.get(hadoopConfig);
            Path filePath = new Path(outputDir + Path.SEPARATOR + "file_" + numFile + ".txt");
            return new BufferedWriter(new OutputStreamWriter(fileSystem.create(filePath), StandardCharsets.UTF_8));
        }

        public void send(String timestamp, String tweet) throws IOException {
            System.out.println("Sending");
            long curTime = currentTimeMillis();

            String strOutput = timestamp + "," + tweet + "\n";
            bw.write(strOutput);
            curFileDim += strOutput.length() * 2;

            if (curTime-lastTime > maxDeltaTime || curFileDim > maxFileDim){
                lastTime = curTime;
                this.flush();
            }
        }

        public void flush() throws IOException {
            bw.flush();
            bw.close();
            curFileDim = 0;
            bw = getNewBufferedWriter();
        }

    }

    public static void main(String args[]) throws IOException { // args[0]: dataset

        if(args.length <= 0){ // if dataset is not specified
            System.err.println("Error: not enough args");
            return;
        }

        // create configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", Globals.hdfsURI);

        // create and start Generator thread
        Generator g = new Generator(conf.get("fs.defaultFS"), args[0], Globals.batchInputPath, Globals.speedInputPath);
        g.start();
    }


}
