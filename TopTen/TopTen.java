//Ryan Temple
package org.apache.hadoop.TopTen;
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

public class TopTen {

    public static class MyMapper
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

                context.write(uid, new Text(movieId + "," + movieRating));
            }
        }
  }

    public static class MyReducer extends Reducer<IntWritable,Text,IntWritable,Text> {
        private IntWritable result = new IntWritable();

        int max = 0;
            StringTokenizer tokenizer = null;
            Float maxRating = Float.valueOf(0);
            IntWritable maxMovieId = null;
            String maxMovieString = "null";
            Text Movie = null;
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            
            

            for (Text value : values) {
                
                tokenizer = new StringTokenizer(value.toString(), ",");
                if (tokenizer.hasMoreTokens()) {
                    String movieId = tokenizer.nextToken();
                    if (tokenizer.hasMoreTokens()) {
                        Float movieRating = Float.valueOf(tokenizer.nextToken());
                        if (movieRating > maxRating) {
                            maxRating = movieRating;
                            Movie = new Text(movieId);
                            
                            // System.out.println("Key: " + key.toString() + " Movie Id: " + Movie.toString());
                            // System.out.println("Set Max Rating:" + maxRating.toString() + " Movie: " + Movie.toString());
                        }
                    }
                }
            }

            context.write(key, Movie);
            // maxRating = Float.valueOf(0);
            // if (Movie.equals(null)) {
            //     context.write(key, Movie);
            // }
            // System.out.println("Writing movie: " + Movie.toString());
            
            // maxRating = 0;

            // for (Text val : values) {
            //     tokenizer = new StringTokenizer(val.toString(), ",");
                // String movieId = tokenizer.nextToken();
                // Float movieRating = Float.valueOf(tokenizer.nextToken());
                // if (movieRating.floatValue() > maxRating) {
                //     maxRating = movieRating.floatValue();
                //     maxMovieId = new IntWritable(Integer.valueOf(movieId));
		        //     Movie = new Text(movieId);//Movie.set(movieId);
                // }
            // }
            // context.write(key, new Text("Yes"));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top Ten");
        job.setJarByClass(TopTen.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}