package Generate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

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

    public Generator(String hdfsURL, String csvPath, String batchInputPath, String speedInputPath) throws IOException {
        this.csvPath = csvPath;
        this.toBatchSender = new Sender(hdfsURL, batchInputPath, 10 * 60 * 1000, 5 * 1024);
        this.toSpeedSender = new Sender(hdfsURL, speedInputPath, 60 * 1000, 1024);
    }

    private List<String[]> readCSV(String sep) throws IOException {
        List<String[]> lines = new LinkedList<String[]>();

        BufferedReader csvReader = new BufferedReader(new FileReader(this.csvPath));
        String row;
        while ((row = csvReader.readLine()) != null) {
            String[] data = row.split(sep, 6);
            lines.add(data);
        }
        csvReader.close();
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
        Collections.shuffle(lines);

        // send tweets to Batch and Speed Layers
        try {
            for (String[] line : lines) {
                toBatchSender.send(line[2], line[5]);
                toSpeedSender.send(line[2], line[5]);
                Thread.sleep((long) (Math.random() * 3));
            }
        }
        catch (IOException ioe){
            System.err.println("Error: IOException");
        } catch (InterruptedException e) {
            System.err.println("Error: InterruptedException");
        }

    }

    private class Sender {

        private long lastTime;
        private String hdfsURL;
        private String outputDir;
        private long maxDeltaTime;
        private long curFileDim;
        private long maxFileDim;
        private int numFile;
        private BufferedWriter bw;

        public Sender(String hdfsURL, String outputDir, long maxDeltaTime, long maxFileDim) throws IOException {
            this.hdfsURL = hdfsURL;
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
            hadoopConfig.set("fs.defaultFS", this.hdfsURL);
            FileSystem fileSystem = FileSystem.get(hadoopConfig);
            Path filePath = new Path(outputDir + Path.SEPARATOR + "file_" + numFile + ".txt");
            return new BufferedWriter(new OutputStreamWriter(fileSystem.create(filePath), StandardCharsets.UTF_8));
        }

        public void send(String timestamp, String tweet) throws IOException {
            long curTime = currentTimeMillis();

            String strOutput = timestamp + "," + tweet;
            bw.write(strOutput);
            curFileDim += strOutput.length() * 2;

            if (curTime-lastTime > maxDeltaTime || curFileDim > maxFileDim){
                bw.close();
                curFileDim = 0;
                lastTime = curTime;
                bw = getNewBufferedWriter();
            }
        }

    }


}
