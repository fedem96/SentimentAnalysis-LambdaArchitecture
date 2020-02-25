# Sentiment Analysis

![alt text](https://github.com/fedem96/SentimentAnalysis-LambdaArchitecture/blob/master/img/architecture.png)

Calculating arbitrary functions on large real-time data sets is a difficult task. In this study a possible approach to problems of this type is shown: as an example task we consider the analysis of the feelings of the tweets. Instead of getting tweets through the Twitter API, we simulate issuing them by reading them from the sentiment140 dataset.
In our scenario we create a Lambda Architecture that uses Apache Hadoop for the Batch Layer, Apache Storm for the Speed
Layer and HDFS for data management. We use LingPipe to classify tweets using computational linguistics.
This architecture allows to harness the full power of a computer cluster for data processing, is easily scalable, and meets low latency requirements for answering queries in real time.

## Software requirements
* Java JDK: open jdk 11
* Apache Hadoop 2.9.2
* Apache Storm 2.1.0
* HdsfSpout Apache Storm 2.1.0
* LingPipe 4.1.2
* jfreechart 1.0.1

## Dataset
Sentiment140 1.6 million annotated sentiment tweets : [download](https://www.kaggle.com/kazanova/sentiment140)

### Download this repo
* `git clone https://github.com/fedem96/SentimentAnalysis-LambdaArchitecture.git`

### Run all the process
Previus import alla jar/library file in your project and test Hadoop configuration.
[Set up a single node cluster guide Hadoop](https://hadoop.apache.org/docs/r2.9.2/hadoop-project-dist/hadoop-common/SingleCluster.html)

* `Classifier` edit configuration and set args[0] = dataset file and args[1] = path/file in which save the classifier
* `Generator` edit configuration and set args[0] = dataset file for generate tweet on hdfs
* `Batch Layer` no parameters need in args
* `Speed Layer` no parameters need in args
* `Query Gui` no parameters in args. run after previus layer
* `Clear` no parameters need in args, used for clear all the directory in hdfs and run a new simulation
