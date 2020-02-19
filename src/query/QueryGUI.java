package query;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.general.DatasetUtilities;
import utils.Globals;

import javax.swing.*;
import java.awt.*;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static query.Query.queryBatch;
import static query.Query.querySpeed;

public class QueryGUI {

    public static void main(String args[]) throws InterruptedException, IOException {

        // create configuration
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", Globals.hdfsURI);
        FileSystem fs = FileSystem.get(conf);

        // create window
        QueryFrame qf = new QueryFrame("Sentiment Analysis - Lambda Architecture");
        qf.setSize(1366, 960);
        qf.setLocationRelativeTo(null);
        qf.setVisible(true);

        // create updater
        new Updater(qf, conf, fs).start();
    }

    private static class QueryFrame extends JFrame{

        private Map<String, QueryPanel> panels;

        JSpinner beginTime, endTime;
        String dateFormatPattern;
        DateFormat dateFormat;

        QueryFrame(String title) throws HeadlessException {
            super(title);

            dateFormatPattern = "yyyy-MM-dd HH:mm";
            dateFormat = new SimpleDateFormat(dateFormatPattern);

            JPanel topPanel = new JPanel();

            topPanel.add(new JLabel("Begin"));
            beginTime = new JSpinner( new SpinnerDateModel() );
            JSpinner.DateEditor beginTimeEditor = new JSpinner.DateEditor(beginTime, dateFormatPattern);
            beginTime.setEditor(beginTimeEditor);
            beginTime.setValue(new Date());
            topPanel.add(beginTime);

            topPanel.add(new JLabel("       End"));
            endTime = new JSpinner( new SpinnerDateModel() );
            JSpinner.DateEditor endTimeEditor = new JSpinner.DateEditor(endTime, dateFormatPattern);
            endTime.setEditor(endTimeEditor);
            endTime.setValue(new Date());
            topPanel.add(endTime);
            
            JPanel centerPanel = new JPanel();
            centerPanel.setLayout(new GridLayout(2, 2));

            panels = new HashMap<>();
            QueryPanel qp = new QueryPanel("total");
            centerPanel.add(qp);
            panels.put("total", qp);
            for(String word: Globals.keywords){
                qp = new QueryPanel(word);
                centerPanel.add(qp);
                panels.put(word, qp);
            }

            this.add(topPanel, BorderLayout.NORTH);
            this.add(centerPanel, BorderLayout.CENTER);
        }

    void setNewData(String word, int batchGood, int batchBad, int speedGood, int speedBad) {
        panels.get(word).setNewData(batchGood, batchBad, speedGood, speedBad);
    }

    private static class QueryPanel extends JPanel{
        private String word;
        private ChartPanel chartPanel;

        public QueryPanel(String word) {
            this.word = word;
            final CategoryDataset dataset = createDataset(0, 0, 0, 0);
            final JFreeChart chart = createChart(dataset);
            chartPanel = new ChartPanel(chart);
            this.add(chartPanel);
        }

        void setNewData(int batchGood, int batchBad, int speedGood, int speedBad) {
            final CategoryDataset dataset = createDataset(batchGood, batchBad, speedGood, speedBad);
            final JFreeChart chart = createChart(dataset);
            this.remove(chartPanel);
            this.chartPanel = new ChartPanel(chart);
            this.add(chartPanel, BorderLayout.CENTER);
            this.revalidate();
        }

        private CategoryDataset createDataset(int batchGood, int batchBad, int speedGood, int speedBad) {
            double[][] data = new double[][]{
                    {batchGood, batchBad},
                    {speedGood, speedBad},
            };
            return DatasetUtilities.createCategoryDataset(new String[]{"Batch", "Speed"}, new String[]{"Good", "Bad"}, data);
        }

        private JFreeChart createChart(final CategoryDataset dataset) {

            final JFreeChart chart = ChartFactory.createStackedBarChart(word, "Tweet sentiment            ", "Number of tweets",
                    dataset, PlotOrientation.VERTICAL, true, true, false);

            chart.setBackgroundPaint(new Color(255, 255, 255));

            CategoryPlot plot = chart.getCategoryPlot();
            plot.getRenderer().setSeriesPaint(0, new Color(0, 81, 149));   // batch color
            plot.getRenderer().setSeriesPaint(1, new Color(249, 135, 68)); // speed color

            return chart;
            }
        }
    }
    


    private static class Updater extends Thread{

        private QueryFrame queryFrame;
        private Configuration conf;
        private FileSystem fs;

        private String beginKey, endKey;

        Updater(QueryFrame queryFrame, Configuration conf, FileSystem fs) {
            this.queryFrame = queryFrame;
            this.conf = conf;
            this.fs = fs;
            updateBeginDate();
            updateEndDate();
            queryFrame.beginTime.addChangeListener(changeEvent -> {updateBeginDate(); doQuery();});
            queryFrame.endTime.addChangeListener(changeEvent -> {updateEndDate(); doQuery();});
        }

        @Override
        public void run() {
            try {
                while (true){
                    doQuery();
                    Thread.sleep(3000);
                }
            }catch (InterruptedException ie){
                System.err.println("Updater interrupted");
            }
        }

        private void doQuery() {
            try {
                HashMap<String, int[]> batchNums = queryBatch(beginKey, endKey, fs, conf);
                HashMap<String, int[]> speedNums = querySpeed(beginKey, endKey, fs);
                
                Set<String> words = new HashSet<>();
                words.addAll(batchNums.keySet());
                words.addAll(batchNums.keySet());
                for(String word: words){
                    queryFrame.setNewData(word, batchNums.get(word)[0], batchNums.get(word)[1], speedNums.get(word)[0], speedNums.get(word)[1]);
                    System.out.println(word);
                    System.out.println("Num good tweets between " + beginKey + " and " + endKey + " (included): " + (batchNums.get(word)[0] + speedNums.get(word)[0]) + " (" + batchNums.get(word)[0] + " from batch, " + speedNums.get(word)[0] + " from speed)");
                    System.out.println("Num bad tweets between " + beginKey + " and " + endKey + " (included): " + (batchNums.get(word)[0] + speedNums.get(word)[0]) + " (" + batchNums.get(word)[1] + " from batch, " + speedNums.get(word)[1] + " from speed)");
                }
                System.out.println();
            }
            catch (IOException ioe){
                ioe.printStackTrace();
            }
        }

        private String convertDate(String dateToConvert){
            SimpleDateFormat dateFormat = new SimpleDateFormat(queryFrame.dateFormatPattern, Locale.ENGLISH);
            SimpleDateFormat timestampFormat = new SimpleDateFormat(Globals.timestampFormatPattern, Locale.ENGLISH);

            try {
                return Globals.timestampToKey(timestampFormat.format(dateFormat.parse(dateToConvert)));
            } catch (ParseException e) {
                e.printStackTrace();
                return null;
            }

        }

        private void updateBeginDate(){
            beginKey = convertDate(queryFrame.dateFormat.format(queryFrame.beginTime.getValue()));
        }

        private void updateEndDate(){
            endKey = convertDate(queryFrame.dateFormat.format(queryFrame.endTime.getValue()));
        }
    }

}

