//Ryan Temple
package org.apache.hadoop.TopTen;
import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.*;

// import org.apache.commons.lang3.StringUtils;

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

public class TopTen {

    public static class TopTenMapper
        extends Mapper<LongWritable,Text,IntWritable,Text>{
        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
            StringTokenizer it = new StringTokenizer(value.toString(), ",");
            while (it.hasMoreTokens()) {
                //Get the token
                String nextToken = it.nextToken();
                IntWritable uid = new IntWritable(Integer.valueOf(nextToken));
                String movieId = it.nextToken();
                String movieRating = it.nextToken();

                context.write(new IntWritable(1), new Text(movieId + "," + movieRating));
            }
        }
  }

    public static class MyReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
       
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int max = 0;
            StringTokenizer tokenizer = null;
            float maxRating = 0;
            IntWritable maxMovieId = null;

            for (Text val : values) {
                tokenizer = new StringTokenizer(val.toString(), ",");
                String movieId = tokenizer.nextToken();
                Float movieRating = Float.valueOf(tokenizer.nextToken());
                if (movieRating.floatValue() > maxRating) {
                    maxRating = movieRating.floatValue();
                    maxMovieId = new IntWritable(Integer.valueOf(movieId));
                }
            }
            context.write(new IntWritable(1), new Text("Hey"));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Ten Movies");
        
        job.setJarByClass(TopTen.class);
        job.setMapperClass(TopTenMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        // job.setMapOutputKeyClass(Text.class);
        // job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
