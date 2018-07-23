package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.StringTokenizer;

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

public class MaxValue {

    static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        // Map interface of the MapReduce job
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] linevalues = value.toString().split(",");

            IntWritable contextKey = new IntWritable(Integer.parseInt(linevalues[0]));
            Text contextValue = new Text(linevalues[1] + "," + linevalues[2]); 
            context.write(contextKey, contextValue);
        }
    }

    static class MyReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        //Shared reference to max rating
        float maxrating = Float.MIN_VALUE;
        //Shared reference to topkey
        IntWritable topKey = new IntWritable(-1);
        //Reference to movie id
        String movie = " ";

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] linevalues;
            float rating;
            
            for (Text value: values) {
                linevalues = value.toString().split(",");
                rating = Float.parseFloat(linevalues[1]); 

                if (rating > maxrating) {
                    maxrating = rating;
                    topKey = key;
		            movie = linevalues[0];  
                }           
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            //Write to context after reducer finished    
            context.write(topKey, new IntWritable(Integer.parseInt(movie)));    
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Value");
        job.setJarByClass(MaxValue.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
} 