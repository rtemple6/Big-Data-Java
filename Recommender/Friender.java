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


public class Friender {
    // Class to implement the mapper interface
    static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        // Map interface of the MapReduce job
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Get the current line
            
            String line = value.toString();
            String[] linevalues = line.split("\t");
            IntWritable movieId = new IntWritable(Integer.parseInt(linevalues[1]));
            Text userId = new Text(linevalues[0]);
            context.write(movieId, userId);
            // for (String v : linevalues) {
            //     System.out.println("Value: " + v);
            // }
            // context.write(new IntWritable(Integer.parseInt(linevalues[0])), new Text(linevalues[1] + "," + linevalues[2]));
        }
    }
    // Class to implement the reducer interface
    static class MyReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        // Reduce interface of the MapReduce job
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Vars to hold the highest values
            String[] linevalues; //hold values
            float rating;

            // Iterate through each value
            System.out.println("Movie Id" + key.toString());
            for (Text value: values) {
                System.out.println("User Id: " + value.toString());
                // linevalues = value.toString().split(",");
                // rating = Float.parseFloat(linevalues[1]); 
               
                // Check if the current rating is more than max_rating
                // if (rating >= 3.0) {
                    // context.write(key, new IntWritable(Integer.parseInt(linevalues[0])));
                // }           
            }
            // context.write(key, new IntWritable(Integer.parseInt(movie)));
        }
    }
    // Main method
    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Friends");
    job.setJarByClass(Friender.class);
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