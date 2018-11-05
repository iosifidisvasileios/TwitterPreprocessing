/**
 * Created by iosifidis on 01.08.16.
 */

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

public final class StatisticsSentimentCorpus {
    static Logger logger = Logger.getLogger(StatisticsSentimentCorpus.class);

    public static class MyMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {

            final String items = row.toString();

            try {

                String[] stringArray = items.split(",");

                final String idKey = String.valueOf(Long.valueOf(stringArray[0].split("\t")[0]));
                String emotion = stringArray[5];
                String preprocessed = stringArray[7];

                context.write(new Text("tweets_parsed"), new IntWritable(1));
                context.write(new Text(emotion), new IntWritable(1));

                if(preprocessed.split(" ").length <= 0){
                    context.write(new Text("#empty_tweets-" + emotion), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 5){
                    context.write(new Text("#tweets_less_than_5"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <=10){
                    context.write(new Text("#tweets_less_than_10"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 20){
                    context.write(new Text("#tweets_less_than_20"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 30){
                    context.write(new Text("#tweets_less_than_30"), new IntWritable(1));
                }else if (preprocessed.split(" ").length <= 50){
                    context.write(new Text("#tweets_less_than_50"), new IntWritable(1));
                }else {
                    context.write(new Text("#tweets_more_than_50"), new IntWritable(1));
                }

            }catch(NumberFormatException e){
                logger.error(e);
//                context.write(new Text("NumberFormatException"),new Text("1"));
//                cnt += 1;
            }catch (ArrayIndexOutOfBoundsException e){
                logger.error(e);
                context.write(new Text("ArrayIndexOutOfBoundsException"),new IntWritable(1));
//                cnt2 += 1;
            }
        }
    }

    public static class MyReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
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
//        configuration.set("mapred.child.java.opts", "-Xmx5");
//        configuration.setBoolean("mapred.output.compress", true);
//        configuration.set("mapred.output.compression.type", SequenceFile.CompressionType.BLOCK.toString());
//        configuration.setClass("mapred.output.compression.codec", GzipCodec.class, CompressionCodec.class);

        if (args.length != 2) {
            logger.error("NEED 2 DIRECTORY ARGUMENTS: -input1(posts/comments) -OUTPUT>");
            System.exit(1);
        }
        Job job = new Job(configuration, "StatisticsSentimentCorpus");

        job.setJarByClass(StatisticsSentimentCorpus.class);

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
