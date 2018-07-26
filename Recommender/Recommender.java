package org.apache.hadoop.ramapo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
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


public class Recommender {
    private static HashMap<Integer, ArrayList<Integer>> userIdLikesList = new HashMap<Integer, ArrayList<Integer>>();
    private static HashMap<Integer, ArrayList<Integer>> friendsList = new HashMap<Integer, ArrayList<Integer>>();
    private static HashMap<Integer, Integer> usersList = new HashMap<Integer,Integer>();
    
    static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] linevalues = line.split(",");
            context.write(new IntWritable(Integer.parseInt(linevalues[0])), new Text(linevalues[1] + "," + linevalues[2]));
        }
    }

    static class MyReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            Integer userId = Integer.parseInt(key.toString());
            //Store list of all users
            usersList.put(userId, 1);
            generateUserLikesList(key, values);
        }

        void generateUserLikesList(IntWritable uid, Iterable<Text> values) {
            String[] linevalues;
            float rating;
            Integer userId = Integer.parseInt(uid.toString());
            // Iterate through each value
            for (Text value: values) {
                linevalues = value.toString().split(",");
                rating = Float.parseFloat(linevalues[1]); 
                // Check if the current rating is more than max_rating
                ArrayList<Integer> list;
                if (rating >= 3.0) {
                    if(userIdLikesList.containsKey(userId)){
                        list = userIdLikesList.get(uid.get());
                        list.add(Integer.valueOf(linevalues[0]));
                    } else {
                        list = new ArrayList<Integer>();
                        list.add(Integer.valueOf(linevalues[0]));
                        userIdLikesList.put(uid.get(), list);
                    }
                }           
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            generateFriendsList();

            //Iterate through users to recommend for each
            for (HashMap.Entry<Integer, Integer> user: usersList.entrySet()){
                Integer uid = user.getKey();
                recommend(uid, context);
            }
        }

        void generateFriendsList() {
            for (HashMap.Entry<Integer, ArrayList<Integer>> user: userIdLikesList.entrySet()) {
                Integer uid = user.getKey();
                ArrayList<Integer> likes = user.getValue();

                //For each like check to see if they have friends :)
                for (Integer like : likes) {
                    for (HashMap.Entry<Integer, ArrayList<Integer>> friend: userIdLikesList.entrySet()) {
                        Integer fid = friend.getKey();
                        ArrayList<Integer> friendLikes = friend.getValue();

                        //Find friends
                        if (uid != fid) {
                            if (friendLikes.contains(like)) {
                                if (friendsList.get(uid) == null) {
                                    friendsList.put(uid, new ArrayList<Integer>());
                                }
                                if (!friendsList.get(uid).contains(fid)){
                                    friendsList.get(uid).add(fid);
                                    // break;
                                }
                            }
                        }
                    }
                }
            }
        }

        void recommend(Integer userId, Context context) throws IOException, InterruptedException{
            ArrayList<Integer> userLikes = new ArrayList<Integer>();
            //Get user likes
            for (HashMap.Entry<Integer, ArrayList<Integer>> user: userIdLikesList.entrySet()) {
                Integer uid = user.getKey();
                if (uid == userId){
                    userLikes = user.getValue();
                }
            }

            for (HashMap.Entry<Integer, ArrayList<Integer>> user: friendsList.entrySet()) {
                Integer uid = user.getKey();
                if (uid == userId){
                    ArrayList<Integer> friends = user.getValue();

                    for (Integer friendId : friends) {
                        for (HashMap.Entry<Integer, ArrayList<Integer>> friend: userIdLikesList.entrySet()) {
                            Integer fid = friend.getKey();
                            if (friendId == fid){
                                ArrayList<Integer> friendLikes = friend.getValue();
                                for (Integer likedMovie: friendLikes){
                                    if (userLikes.contains(likedMovie)){
                                        System.out.println("User already like this movie");
                                    } else {
                                        context.write(new IntWritable(uid), new IntWritable(likedMovie));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }


    // Main method
    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "recommenderjob");
    job.setJarByClass(Recommender.class);
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