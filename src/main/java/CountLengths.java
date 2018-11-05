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

public final class CountLengths {

    static Logger logger = Logger.getLogger(CountLengths.class);

         public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{

             public void map(Object key, Text row, Context context) throws IOException, InterruptedException {

            final String items = row.toString();
            try {

                String[] stringArray = items.split(",");
                String preprocessed = stringArray[7];

                if(preprocessed.split(" ").length == 0){
                    context.write(new Text("#empty_tweets"), new IntWritable(1));
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
                    context.write(new Text("#tweets_More_than_50"), new IntWritable(1));

                }

            }catch(NumberFormatException e){
                logger.error(e);
                logger.error(items);
            }catch (ArrayIndexOutOfBoundsException e){
                logger.error(e);
            }
        }
    }
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Count Lengths");

        job.setJarByClass(CountLengths.class);
        job.setMapperClass(MyMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
