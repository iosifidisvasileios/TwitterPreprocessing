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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public final class CountTweetsPerDay {
    static Logger logger = Logger.getLogger(CountTweetsPerDay.class);
    static final SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
    static final SimpleDateFormat sdf_2 = new SimpleDateFormat("yyyy-MM");

    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{

        public void map(Object key, Text row, Context context) throws IOException, InterruptedException {
            final String items = row.toString();
            try {
                final String[] stringArray = items.split(",");

                final String dateTemp = stringArray[1];
                final Date date = sdf.parse(dateTemp);
                final String sentiment = stringArray[3];

                context.write(new Text(sentiment  + ": " + sdf_2.format(date)) , new IntWritable(1));


            }catch (ArrayIndexOutOfBoundsException e){
                logger.error(e,e);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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
        Configuration configuration = new Configuration();
        if (args.length != 2) {
            logger.error("NEED 2 DIRECTORY ARGUMENTS: -input1(posts/comments) -OUTPUT>");
            System.exit(1);
        }
        Job job = new Job(configuration, "CountTweetsPerMonth");

        job.setJarByClass(CountTweetsPerDay.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
