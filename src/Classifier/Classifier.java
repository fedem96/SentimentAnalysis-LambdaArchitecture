package Classifier;

import com.aliasi.stats.MultivariateEstimator;
import com.aliasi.classify.*;
import com.aliasi.lm.NGramProcessLM;

import java.io.*;

public class Classifier {

     private DynamicLMClassifier<NGramProcessLM> trainedClassifier;
     private LMClassifier<NGramProcessLM, MultivariateEstimator> mClassifier;

    public Classifier(){
        trainedClassifier = null;
        mClassifier = null;
    }

    public Classifier(String modelPath) throws IOException, ClassNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(modelPath);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
        ObjectInputStream objectInputStream = new ObjectInputStream(bufferedInputStream);
        Object object = objectInputStream.readObject();
        objectInputStream.close();
        mClassifier = (LMClassifier<NGramProcessLM, MultivariateEstimator>)object;
    }

    public void train(String datasetPath) throws IOException {
        //define params for classification
        String[] mCategories = {"neg","pos"};
        trainedClassifier = DynamicLMClassifier.createNGramProcess(mCategories,2);
        System.out.println("\nTraining.");

        FileReader fileReader = new FileReader(datasetPath);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        // Train the classifier
        String line = "", sentiment="";

        while((line = bufferedReader.readLine()) != null) {
            String[] s = line.split(",");
            if(s[0].contains("0")) {
                sentiment = "neg";
            }else{
                sentiment = "pos";
            }
            Classification classification = new Classification(sentiment);
            Classified<CharSequence> classified = new Classified<CharSequence>(s[5], classification);
            trainedClassifier.handle(classified);
        }
        bufferedReader.close();
    }

    public void save(String outputPath) throws IOException {
        // save the local classifier
        FileOutputStream fout = new FileOutputStream(outputPath + "classifier_weights.lpc");
        ObjectOutputStream oos = new ObjectOutputStream(fout);
        trainedClassifier.compileTo(oos);
        oos.close();
    }

    public void evaluate(String datasetpath) throws IOException {
        System.out.println("\nEvaluating.");
        int numTests = 0;
        int numCorrect = 0;

        String line = "", categ="";
        FileReader fileReader = new FileReader(datasetpath);
        BufferedReader bufferedReader = new BufferedReader(fileReader);

        while((line = bufferedReader.readLine()) != null) {
            numTests += 1;
            System.out.print("Num of test: " + numTests +" \r");
            String[] element = line.split(",", 2);
            if(element[0].contains("0")) {
                categ = "neg";
            }else{
                categ = "pos";
            }

            if(evaluateTweet(element[0]).equals(categ))
                numCorrect += 1;
        }

        bufferedReader.close();
        // ling pipe example code
        System.out.println("  # Test Cases=" + numTests);
        System.out.println("  # Correct=" + numCorrect);
        System.out.println("  % Correct="
                + ((double)numCorrect)/(double)numTests);
    }


    public String evaluateTweet(String text){
        Classification classification = mClassifier.classify(text);
        return classification.bestCategory();
    }


    // args[0] = path of dataset for training
    // args[1] = path for save classifier weights
    /*  my case
        /home/iacopo/Scrivania/SentimentAnalysis-LambdaArchitecture/dataset/training.1600000.processed.noemoticon.csv
        /home/iacopo/Scrivania/SentimentAnalysis-LambdaArchitecture/dataset/
     */
    public static void main(String args[]) throws IOException, ClassNotFoundException {
        // create the classifier and save weights
        File file = new File(args[1]+"classifier_weights.lpc");
        if (!file.exists())
        {
            Classifier classifier = new Classifier();
            classifier.train(args[0]);
            // SMALLER DATASET
            //classifier.train("/home/iacopo/Scrivania/SentimentAnalysis-LambdaArchitecture/dataset/small.csv");
            classifier.save(args[1]);
        }

        // evaluation debug
        // TODO accuracy 100% on train dataset ok but make some test whit a smaller train dataset
        Classifier c = new Classifier(args[1]+"classifier_weights.lpc");
        c.evaluate(args[0]);
        //c.evaluate("/home/iacopo/Scrivania/SentimentAnalysis-LambdaArchitecture/dataset/small.csv");


        // real positive tweet in the dataset
        String text = "I LOVE @Health4UandPets u guys r the best!! ";
        Classifier pol = new Classifier(args[1]+"classifier_weights.lpc");
        System.out.println(text);
        System.out.println(pol.evaluateTweet(text));

        //TODO why this is neg ?!?!?!
        text = "I am happy happy !!!!!!!!! ";
        System.out.println(text);
        System.out.println(pol.evaluateTweet(text));


    }

}