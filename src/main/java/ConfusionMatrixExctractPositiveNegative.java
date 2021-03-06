 /**
 * Created by iosifidis on 01.08.16.
 */

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.io.IOException;
import java.util.HashMap;

public final class ConfusionMatrixExctractPositiveNegative {
    static Logger logger = Logger.getLogger(ConfusionMatrixExctractPositiveNegative.class);
    public static final HashMap<String,Double> sentiDictionary = new SentiwordLoader().getDictionary();
    public static final MaxentTagger tagger = new TaggerLoader().getTagger();
    public static final EnglishStemmer english = new EnglishStemmer();


    public static class MyMapper extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {

            final String items = row.toString();
//            try {
            String after_preprocessed;

            String[] stringArray = items.split(",");


//                if (!stringArray[2].equals("null")) {
//                    after_preprocessed = stringArray[10];
//                    emoticon = stringArray[8];
//                } else {
//                    after_preprocessed = stringArray[8];
//                    emoticon = stringArray[6];
//                }
            try {
                String emoticon = stringArray[5];
                after_preprocessed = stringArray[7];


                if (emoticon.equals("positive") || emoticon.equals("negative")) {
                    String tmp = "";
                    int counter = 0;
                    double score = 0.0;

                    for (String word : after_preprocessed.split(" ")) {
                        boolean flag = false;
                        if (word.contains("_")) {
                            flag = true;
                            word = word.split("_")[1];
                        }
                        String temporal = tagger.tagString(word);
                        if (flag) {
                            temporal = "NOT_" + temporal;
                        }
                        tmp += temporal;
//                        tmp += tagger.tagString(word);
                    }

                    for (String word : tmp.trim().split(" ")) {
                        boolean flag_2 = false;
                        if (word.split("_").length == 3) {
//                        logger.info(word);
                            flag_2 = true;
                            word = word.replace("NOT_", "");
                        }

                        String term = word.split("_")[1];
                        String typeOfSpeech;

                        switch (term) {
                            case "JJ":
                            case "JJR":
                            case "JJS":
                                typeOfSpeech = "a";
                                break;
                            case "NN":
                            case "NNS":
                            case "NNP":
                            case "NNPS":
                                typeOfSpeech = "n";
                                break;
                            case "RB":
                            case "RBR":
                            case "RBS":
                                typeOfSpeech = "r";
                                break;
                            case "VB":
                            case "VBD":
                            case "VBG":
                            case "VBN":
                            case "VBP":
                            case "VBZ":
                                typeOfSpeech = "v";
                                break;
                            default:
                                typeOfSpeech = "null";
                        }

                        if (!typeOfSpeech.equals("null")) {
                            if (sentiDictionary.containsKey(word.split("_")[0] + "#" + typeOfSpeech)) {
                                if (flag_2) {
                                    score -= sentiDictionary.get(word.split("_")[0] + "#" + typeOfSpeech);
                                } else {
                                    score += sentiDictionary.get(word.split("_")[0] + "#" + typeOfSpeech);
                                }
                                counter++;
                            }
                        }
                    }
                    if (score != 0.0) {
                        score /= Double.valueOf(counter);
                    }
                    final String idKey = String.valueOf(Long.valueOf(stringArray[0].split("\t")[0]));

                    String stemmed = "";
                    for(String word : stringArray[7].split(" ")){
                        english.setCurrent(word);
                        english.stem();
                        stemmed += english.getCurrent() + " ";
                    }
                    stemmed = stemmed.substring(0, stemmed.length() - 1);

                    final String outpout = "," + stringArray[1] + "," + stringArray[2] + "," + stringArray[3] + "," + stringArray[4] + "," + stringArray[5] + "," + stringArray[6] + "," + stemmed;
//                final String idKey = String.valueOf(Long.valueOf(stringArray[0].split("\t")[0]));
                    confusionArray(score, emoticon, context, idKey, outpout);
                }
            }catch (ArrayIndexOutOfBoundsException e){
                logger.error(e,e);
            }
//
//            }catch(NumberFormatException e){
//                context.write(new Text("NumberFormatException"),new IntWritable(1));
//            }
//            catch (ArrayIndexOutOfBoundsException e){
//                context.write(new Text("ArrayIndexOutOfBoundsException"),new IntWritable(1));
//            }

        }

        private static void confusionArray(double score, String emoticon, Context context, String idKey, String outpout) throws IOException, InterruptedException {

            if( score > 0){

                if (emoticon.equals("positive")){
                    context.write(new Text(idKey), new Text(outpout));
                }
            }
            else if(score < 0 ){
                if (emoticon.equals("negative")){
                    context.write(new Text(idKey), new Text(outpout));
                }
            }

        }



    }


    static class MyReducer extends Reducer<Text, Text, Text, Text>{
        //        Map<String,String> entries = new HashMap<String, String>();
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(values.iterator().next()));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        if (args.length != 2) {
            logger.error("NEED 2 DIRECTORY ARGUMENTS: -input1(posts/comments) -OUTPUT>");
            System.exit(1);
        }
        Job job = new Job(configuration, "ConfusionMatrixExctractPositiveNegative");

        job.setJarByClass(ConfusionMatrixExctractPositiveNegative.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
