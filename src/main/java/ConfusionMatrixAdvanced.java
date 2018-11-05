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

import java.io.IOException;
import java.util.HashMap;

public final class ConfusionMatrixAdvanced {
    static Logger logger = Logger.getLogger(ConfusionMatrixAdvanced.class);
    public static final HashMap<String,Double> sentiDictionary = new SentiwordLoader().getDictionary();
    public static final MaxentTagger tagger = new TaggerLoader().getTagger();


    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {

            final String items = row.toString();
            String after_preprocessed;

            String[] stringArray = items.split(",");

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

                    int threshold = tmp.trim().split(" ").length;
                    int notFound = 0;
                    boolean neutral = true;

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

                                if(sentiDictionary.get(word.split("_")[0] + "#" + typeOfSpeech) != 0){
                                    neutral = false;
                                }


                            }else{
                                notFound+=1;
                            }
                        }else {
                            notFound+=1;
                        }
                    }
                    if (score != 0.0) {
                        score /= Double.valueOf(counter);
                    }
                    String myString = "";

                    if (notFound == threshold && score == 0){
                        myString = "NonExisting";
                    }
                    else if(neutral && score == 0){
                        myString = "NeutralWords";
                    }
                    else if(score == 0 && notFound != threshold ){
                        myString = "SumIsZero";
                    }

                    confusionArray(score, emoticon, context, myString);
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

        private static void confusionArray(double score, String emoticon, Context context, String myString) throws IOException, InterruptedException {

            if (score == 0 ){
                context.write(new Text("Neutral-" + myString + "-" + emoticon), new IntWritable(1));
            }
            else if( score > 0){

                if (emoticon.equals("positive")){
                    context.write(new Text("True_Positive"), new IntWritable(1));
                }
                else{
                    context.write(new Text("False_Negative"), new IntWritable(1));
                }
            }
            else{

                if (emoticon.equals("negative")){
                    context.write(new Text("True_Negative"), new IntWritable(1));
                }
                else{
                    context.write(new Text("False_Positive"), new IntWritable(1));
                }
            }

        }



    }


    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        if (args.length != 2) {
            logger.error("NEED 2 DIRECTORY ARGUMENTS: -input1(posts/comments) -OUTPUT>");
            System.exit(1);
        }
        Job job = new Job(configuration, "ConfusionMatrix");

        job.setJarByClass(ConfusionMatrixAdvanced.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
