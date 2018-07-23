//Ryan Temple
package org.apache.hadoop.maxtemp;
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;

import org.apache.commons.lang3.StringUtils;

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

public class MaxTemp {

    public static class MyMapper
        extends Mapper<LongWritable, Text, Text, IntWritable>{

        private IntWritable temperature = new IntWritable();
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString());
            while (it.hasMoreTokens()) {
                //Get the token
                String nextToken = it.nextToken();

                //Check if it is number or text
                if (StringUtils.isNumeric(nextToken)) {
                    //Set and write to context
                    temperature.set(Integer.parseInt(nextToken));
                    context.write(word, temperature);
                } else {
                    //Set
                    word.set(nextToken);
                } 
            }
        }
  }

    public static class MonthTempReducer extends Reducer<Text,IntWritable,Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
            int max = 0;
            for (IntWritable val : values) {
                max = Math.max(max, val.get());
            }
            result.set(max);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max temperature");
        job.setJarByClass(MaxTemp.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MonthTempReducer.class);
        job.setReducerClass(MonthTempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
