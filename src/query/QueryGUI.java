package query;


import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.CategoryPlot;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.category.CategoryDataset;
import org.jfree.data.general.DatasetUtilities;

import javax.swing.*;
import java.awt.*;
import java.util.Date;

public class QueryGUI {

    public static void main(String args[]){
        JFrame f = new QueryFrame("Sentiment Analysis - Lambda Architecture");

        f.setSize(600, 540);
        f.setLocationRelativeTo(null);
        f.setVisible(true);
    }

    private static CategoryDataset createDataset() {
        double[][] data = new double[][]{
                {210, 300},
                {20, 34},
        };
        return DatasetUtilities.createCategoryDataset(new String[]{"Batch", "Speed"}, new String[]{"Good", "Bad"}, data);
    }

    private static JFreeChart createChart(final CategoryDataset dataset) {

        final JFreeChart chart = ChartFactory.createStackedBarChart(
                "Sentiment Analysis", "Tweet sentiment            ", "Number of tweets",
                dataset, PlotOrientation.VERTICAL, true, true, false);

        chart.setBackgroundPaint(new Color(255, 255, 255));

        CategoryPlot plot = chart.getCategoryPlot();
        plot.getRenderer().setSeriesPaint(0, new Color(0, 81, 149));   // batch color
        plot.getRenderer().setSeriesPaint(1, new Color(249, 135, 68)); // speed color

        return chart;
    }

    private static class QueryFrame extends JFrame{

        public QueryFrame(String title) throws HeadlessException {
            super(title);

            JPanel topPanel = new JPanel();

            topPanel.add(new JLabel("Begin"));
            JSpinner beginTime = new JSpinner( new SpinnerDateModel() );
            JSpinner.DateEditor beginTimeEditor = new JSpinner.DateEditor(beginTime, "yyyy-MM-dd HH:mm");
            beginTime.setEditor(beginTimeEditor);
            beginTime.setValue(new Date());
            topPanel.add(beginTime);

            topPanel.add(new JLabel("       End"));
            JSpinner endTime = new JSpinner( new SpinnerDateModel() );
            JSpinner.DateEditor endTimeEditor = new JSpinner.DateEditor(endTime, "yyyy-MM-dd HH:mm");
            endTime.setEditor(endTimeEditor);
            endTime.setValue(new Date());
            topPanel.add(endTime);

            final CategoryDataset dataset = createDataset();
            final JFreeChart chart = createChart(dataset);
            final ChartPanel chartPanel = new ChartPanel(chart);

            this.add(topPanel, BorderLayout.NORTH);
            this.add(chartPanel, BorderLayout.CENTER);

        }
    }

}

