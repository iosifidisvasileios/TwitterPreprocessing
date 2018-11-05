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

public final class StatisticsEnglishCorpusForDistinct {

    private static final ProcessingOfRow processingOfRow = new ProcessingOfRow();
    static Logger logger = Logger.getLogger(StatisticsEnglishCorpusForDistinct.class);

    public static class MyMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {

            final String items = row.toString();

            try {
                String preprocessed = "";
                String outpout = "";

                String[] stringArray = items.split(",");

                final String idKey = String.valueOf(Long.valueOf(stringArray[0].split("\t")[0]));

                preprocessed = stringArray[7];
//                preprocessed = stringArray[3];
//                context.write(new Text("tweets_parsed"), new IntWritable(1));
//                preprocessed = processingOfRow.clearWhitespace(preprocessed);
//                for(String word : preprocessed.split(" ")){
//                    if(word.contains(",")){word = word.split(",")[0];}
//                    context.write(new Text( word ), new IntWritable(1));
//                }
//
//                if(preprocessed.split(" ").length == 0){
//                    context.write(new Text("#before_empty_tweets"), new IntWritable(1));
//                }else if (preprocessed.split(" ").length <= 3){
//                    context.write(new Text("#before_tweets_less_than_3"), new IntWritable(1));
//                }else if (preprocessed.split(" ").length <= 5){
//                    context.write(new Text("#before_tweets_less_than_5"), new IntWritable(1));
//                }else if (preprocessed.split(" ").length <=10){
//                    context.write(new Text("#before_tweets_less_than_10"), new IntWritable(1));
//                }else if (preprocessed.split(" ").length <= 20){
//                    context.write(new Text("#before_tweets_less_than_20"), new IntWritable(1));
//                }else if (preprocessed.split(" ").length <= 30){
//                    context.write(new Text("#before_tweets_less_than_30"), new IntWritable(1));
//                }else if (preprocessed.split(" ").length <= 50){
//                    context.write(new Text("#before_tweets_less_than_50"), new IntWritable(1));
//                }else {
//                    context.write(new Text("#before_tweets_more_than_50"), new IntWritable(1));
//                }
/*
                //before deletion of some characters do a slang check
                // 2
                preprocessed = processingOfRow.slangProcess(preprocessed.toLowerCase());
//                for(String word : preprocessed.split(" ")){
//                    if(word.contains(",")){word = word.split(",")[0];}
//                    context.write(new Text( word ), new IntWritable(1));
//                }

//              3
                preprocessed = processingOfRow.clearText(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
//                for(String word : preprocessed.split(" ")){
//                    if(word.contains(",")){word = word.split(",")[0];}
//                    context.write(new Text( word ), new IntWritable(1));
//                }

                //lowercase words, remove repetitions, replace slang terms
                // 4
                preprocessed = processingOfRow.slangProcess(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
//                for(String word : preprocessed.split(" ")){
//                    if(word.contains(",")){word = word.split(",")[0];}
//                    context.write(new Text( word ), new IntWritable(1));
//                }

                //count emoticons, store them into string
//                final EmoticonResult happyResult = processingOfRow.happyCounting(preprocessed);
//                final EmoticonResult sadResult = processingOfRow.sadCounting(preprocessed);


                //count emoticons and store them into string
                // 5
                preprocessed = processingOfRow.negationsBasedOnVerbs(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
//                for(String word : preprocessed.split(" ")){
//                    if(word.contains(",")){word = word.split(",")[0];}
//                    context.write(new Text( word ), new IntWritable(1));
//                }

                // 6
                preprocessed = processingOfRow.negationsBasedOnAdj(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
//                for(String word : preprocessed.split(" ")){
//                    if(word.contains(",")){word = word.split(",")[0];}
//                    context.write(new Text( word ), new IntWritable(1));
//                }

                //remove emoticons, non ascii chars, symbols
                // 7
                preprocessed = processingOfRow.removeNonAscii(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
//                for(String word : preprocessed.split(" ")){
//                    if(word.contains(",")){word = word.split(",")[0];}
//                    context.write(new Text( word ), new IntWritable(1));
//                }

                // 8
                preprocessed = processingOfRow.removeStopwords(preprocessed);
                preprocessed = processingOfRow.clearWhitespace(preprocessed);
                for(String word : preprocessed.split(" ")){
                    if(word.contains(",")){word = word.split(",")[0];}
                    context.write(new Text( word ), new IntWritable(1));
                }
*/
                String emotion ="";
                String emoticonString="";

                if(preprocessed.split(" ").length == 0){
                    context.write(new Text("#after_empty_tweets"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 3){
                    context.write(new Text("#after_tweets_less_than_3"), new IntWritable(1));
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
/*
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
*/

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

    public static class MyReducer extends Reducer<Text,IntWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable();
//        final HashSet<String> total= new HashSet<>();
//        final HashSet<String> slang1= new HashSet<>();
//        final HashSet<String> links= new HashSet<>();
//        final HashSet<String> slang2= new HashSet<>();
//        final HashSet<String> verbs= new HashSet<>();
//        final HashSet<String> adjs= new HashSet<>();
//        final HashSet<String> emojis= new HashSet<>();
//        final HashSet<String> stopwordsLoader= new HashSet<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            long sum = 0;

                for (IntWritable val : values) {

//                    if(key.toString().contains(",")) {
//                        String term = key.toString().split(",")[0];
//                        String tag = key.toString().split(",")[1];
//
//                        switch (tag) {
//                            case "total":
//                                if (!total.contains(term)) {
//                                    total.add(term);
//                                }
//                                break;
//                            case "slang1":
//                                if (!slang1.contains(term)) {
//                                    slang1.add(term);
//                                }
//                                break;
//                            case "links":
//                                if (!links.contains(term)) {
//                                    links.add(term);
//                                }
//
//                                break;
//                            case "slang2":
//                                if (!slang2.contains(term)) {
//                                    slang2.add(term);
//                                }
//                                break;
//                            case "verbs":
//                                if (!verbs.contains(term)) {
//                                    verbs.add(term);
//                                }
//
//                                break;
//                            case "adjs":
//                                if (!adjs.contains(term)) {
//                                    adjs.add(term);
//                                }
//
//                                break;
//                            case "emojis":
//                                if (!emojis.contains(term)) {
//                                    emojis.add(term);
//                                }
//
//                                break;
//                            case "stopwordsLoader":
//                                if (!stopwordsLoader.contains(term)) {
//                                    stopwordsLoader.add(term);
//                                }
//                                break;
//                            default:
//                                break;
//                        }
//
//                    }else {

                        sum += val.get();
//                    }
                }
            result.set(sum);
//            if(!key.toString().contains(",")){
            context.write(key, result);
//            }

            }

        @Override
        protected void cleanup(final Context context) throws IOException, InterruptedException {
//            context.write(new Text("total"), new LongWritable(total.size()));
//            context.write(new Text("slang1"), new LongWritable(slang1.size()));
//            context.write(new Text("links"), new LongWritable(links.size()));
//            context.write(new Text("slang2"), new LongWritable(slang2.size()));
//            context.write(new Text("verbs"), new LongWritable(verbs.size()));
//            context.write(new Text("adjs"), new LongWritable(adjs.size()));
//            context.write(new Text("emojis"), new LongWritable(emojis.size()));
//            context.write(new Text("stopwordsLoader"), new LongWritable(stopwordsLoader.size()));

        }

    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
//        configuration.set("mapred.child.java.opts", "-Xmx5");
//        configuration.setBoolean("mapred.output.compress", true);
//        configuration.set("mapred.output.compression.type", SequenceFile.CompressionType.BLOCK.toString());
//        configuration.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);
//        configuration.set("mapreduce.reduce.memory.mb","33686");
//        configuration.set("mapreduce.reduce.java.opts","-Xmx33686m");
//        configuration.set("mapred.job.reduce.memory.mb","-Xmx33686m");
//        configuration.set("mapred.job.reduce.memory.mb","33686");
//        if (args.length != 2) {
//            logger.error("NEED 2 DIRECTORY ARGUMENTS: -input1(posts/comments) -OUTPUT>");
//            System.exit(1);
//        }
        Job job = new Job(configuration, "StatisticsEnglishCorpusForDistinct");

        job.setJarByClass(StatisticsEnglishCorpusForDistinct.class);
        job.setNumReduceTasks(1);

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
