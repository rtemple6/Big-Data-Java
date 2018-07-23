
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

public class wordfreq {

  public static class MyMapper
       extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer it = new StringTokenizer(value.toString());
      while (it.hasMoreTokens()) {
        word.set(it.nextToken());
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
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
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word frequency");
    job.setJarByClass(wordfreq.class);
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

// //mapreduce code to find number of occurences of each word in a file
// //create a textfile in usr/local/hadoop folder
// //usr/local/hadoop/share/hadoop/mapreduce
// //within the mapreduce folder create a folder mymapreduce
// //create a textfile name "wordfreq.java" within the mymapreduce folder
// //PATH: /usr/local/hadoop/share/hadoop/mapreduce/mymapreduce/
// package org.apache.hadoop.example;

// import java.io.IOException; //support madreduce exception
// import java.util.StringTokenizer;

// //import com.sun.tools.javac.api.DiagnosticFormatter.Configuration;

// import org.apache.hadoop.conf.Configuration; //config file for
// import org.apache.hadoop.fs.Path; //import file system path from apache
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.IntWritable; //used for map api / reduce api
// import org.apache.hadoop.io.Text; //For distributed systems use Text since it is thread safe
// import org.apache.hadoop.mapreduce.Job; //Import Scheduler
// import org.apache.hadoop.madreduce.Mapper;  // MAP API
// import org.apache.hadoop.madreduce.Reducer;  // Reduce API
// import org.apache.hadoop.madreduce.lib.input.FileInputFormat;  // support -csv, .text
// import org.apache.hadoop.madreduce.lib.output.FileOutputFormat; // support 

// //1. Map 2. Reduce 3. Driver
// //Flow: Driver->map(<Key, Value>)->Context->Reduce(Key, value)->output file (part-000, part001,..)

// /**
//  * wordfreq
//  */
// public class wordfreq {
//     //Input.key, Input.value, Output.key, Output.value
//     public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
//         private final static IntWritable one = new IntWritable(1);
//         private Text word = new Text();

//         public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//             StringTokenizer it = StringTokenizer(value.toString());
//             while(it.hasMoreTokens()) {
//                 word.set(it.nextToken());
//                 context.write(word, one);
//             }
//         }
//     }

//     public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
//         private IntWritable result = new IntWritable();
//         private Text word = new Text();

//         public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//             int sum = 0;
//             for (IntWritable value : values) {
//                 sum += value.get();

//             }
//             result.set(sum);
//             context.write(key, result);
//         }
//     }

//     public static void main(String[] args) throws Exception {
//         Configuration conf = new Configuration();
//         Job job = Job.getInstance(conf, "word frequency");
//         job.setJarByClass(wordfreq.class);
//         job.setMapperClass(MyMapper.class);
//         job.setCombinerClass(MyReducer.class);
//         job.setReducerClass(MyReducer.class);
//         job.setOutputKeyClass(Text.class);
//         job.setOutputValueClass(IntWritable.class);

//         FileInputFormat.addInputPath(job, new path(args[0]));
//         FileOutputFormat.setOutputPath(job, new path(args[1]));

//         System.exit(job.waitForCompletion(true)? 0: 1);
//     }

// }

// //Navigate to hadoop folder in terminal
