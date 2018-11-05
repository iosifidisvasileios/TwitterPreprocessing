/**
 * Created by iosifidis on 01.08.16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public final class StatisticsEnglishCorpus {

    private static final ProcessingOfRow processingOfRow = new ProcessingOfRow();
    static Logger logger = Logger.getLogger(StatisticsEnglishCorpus.class);

    public static class MyMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {

            final String items = row.toString();

            try {
                String preprocessed = "";
//                String outpout = "";

                String[] stringArray = items.split(",");

                final String idKey = String.valueOf(Long.valueOf(stringArray[0].split("\t")[0]));

                preprocessed = stringArray[3];
//                outpout = "," + stringArray[1] + "," + stringArray[2] + "," + stringArray[3]+ "," + stringArray[4];
               // context.write(new Text("tweets_parsed"), new IntWritable(1));

                preprocessed = processingOfRow.clearWhitespace(preprocessed);
//                context.write(new Text("total words"), new IntWritable(preprocessed.split(" ").length));
/*

                if(preprocessed.split(" ").length == 0){
                    context.write(new Text("#before_empty_tweets"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 3){
                    context.write(new Text("#before_tweets_less_than_3"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 5){
                    context.write(new Text("#before_tweets_less_than_5"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <=10){
                    context.write(new Text("#before_tweets_less_than_10"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 20){
                    context.write(new Text("#before_tweets_less_than_20"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 30){
                    context.write(new Text("#before_tweets_less_than_30"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 50){
                    context.write(new Text("#before_tweets_less_than_50"), new IntWritable(1));
                }else {
                    context.write(new Text("#before_tweets_more_than_50"), new IntWritable(1));
                }
*/
                //before deletion of some characters do a slang check
                preprocessed = processingOfRow.slangProcess(preprocessed.toLowerCase());
//                context.write(new Text("after_slang_process"), new IntWritable(preprocessed.split(" ").length));

                preprocessed = processingOfRow.clearText(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
//                context.write(new Text("after_remove_links_references"), new IntWritable(preprocessed.split(" ").length));

                //lowercase words, remove repetitions, replace slang terms
                preprocessed = processingOfRow.slangProcess(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
//                context.write(new Text("after_slang_process_2"), new IntWritable(preprocessed.split(" ").length));

                //count emoticons, store them into string
                final EmoticonResult happyResult = processingOfRow.happyCounting(preprocessed);
                final EmoticonResult sadResult = processingOfRow.sadCounting(preprocessed);

/*
                //count emoticons and store them into string
                preprocessed = processingOfRow.negationsBasedOnVerbs(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
//                context.write(new Text("after_negation_VERBS_process"), new IntWritable(preprocessed.split(" ").length));

                preprocessed = processingOfRow.negationsBasedOnAdj(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
                context.write(new Text("after_negation_ADJECTIVES_process"), new IntWritable(preprocessed.split(" ").length));

                //remove emoticons, non ascii chars, symbols
                preprocessed = processingOfRow.removeNonAscii(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
                context.write(new Text("after_remove_specialChars_emoticons"), new IntWritable(preprocessed.split(" ").length));

                preprocessed = processingOfRow.removeStopwords(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
                context.write(new Text("after_remove_stopwords"), new IntWritable(preprocessed.split(" ").length));

                if(preprocessed.split(" ").length == 0){
                    context.write(new Text("#after_empty_tweets"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 5){
                    context.write(new Text("#after_tweets_less_than_5"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <=10){
                    context.write(new Text("#after_tweets_less_than_10"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 20){
                    context.write(new Text("#after_tweets_less_than_20"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 30){
                    context.write(new Text("#after_tweets_less_than_30"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 50){
                    context.write(new Text("#after_tweets_less_than_50"), new IntWritable(1));
                }else {
                    context.write(new Text("#after_tweets_more_than_50"), new IntWritable(1));

                }
*/
                String emotion ="";
                String emoticonString="";

                if (happyResult.getCounter() == 0 && sadResult.getCounter() == 0){
                    emotion = "neutral";
                    context.write(new Text("#tweets_emoticons_neutral"), new IntWritable(1));
                }else if(happyResult.getCounter() > 0 && sadResult.getCounter() == 0){
                    context.write(new Text("#tweets_emoticons_positive"), new IntWritable(1));
                    emotion = "positive";
                }else if(sadResult.getCounter() > 0 && happyResult.getCounter() == 0){
                    context.write(new Text("#tweets_emoticons_negative"), new IntWritable(1));
                    emotion = "negative";
                }else if(happyResult.getCounter() > 0 && happyResult.getCounter() > sadResult.getCounter()){
                    context.write(new Text("#tweets_emoticons_semiPositive"), new IntWritable(1));
                    emotion = "semi_positive";
                }else if(sadResult.getCounter() > 0 && sadResult.getCounter()> happyResult.getCounter()){
                    context.write(new Text("#tweets_emoticons_semiNegative"), new IntWritable(1));
                    emotion = "semi_negative";
                }else{
                    context.write(new Text("#tweets_emoticons_mixed"), new IntWritable(1));
                    emotion = "unknown";
                }

            }catch(NumberFormatException e){
                logger.error(e);
//                context.write(new Text("NumberFormatException"),new Text("1"));
//                cnt += 1;
            }catch (ArrayIndexOutOfBoundsException e){
                logger.error(e);
//                context.write(new Text("ArrayIndexOutOfBoundsException"),new Text("1"));
//                cnt2 += 1;
            }
        }
    }

    public static class MyReducer
            extends Reducer<Text,IntWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            long sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set("mapred.child.java.opts", "-Xmx5");
//        configuration.setBoolean("mapred.output.compress", true);
//        configuration.set("mapred.output.compression.type", SequenceFile.CompressionType.BLOCK.toString());
//        configuration.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);

        if (args.length != 2) {
            logger.error("NEED 2 DIRECTORY ARGUMENTS: -input1(posts/comments) -OUTPUT>");
            System.exit(1);
        }
        Job job = new Job(configuration, "StatisticsEnglishCorpus");

        job.setJarByClass(StatisticsEnglishCorpus.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
