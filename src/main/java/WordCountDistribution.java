/**
 * Created by iosifidis on 08.08.16.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;

public class WordCountDistribution {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable>{
        static Logger logger = Logger.getLogger(WordCountDistribution.class);

        private final static LongWritable one = new LongWritable(1);

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
            final String items = row.toString();

            try {

                final String idKey = String.valueOf(Long.valueOf(items.split("\t")[1]));
                context.write(new Text(idKey + ":"), one);

            }catch(NumberFormatException e){
                logger.error(e);
            }catch (ArrayIndexOutOfBoundsException e){
                logger.error(e);
            }

        }
    }

    public static class IntSumReducer extends Reducer<Text,LongWritable,Text,LongWritable> {
        private LongWritable result = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "WordCountDistribution");

        job.setJarByClass(WordCountDistribution.class);
        job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}