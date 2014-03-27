import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HW2Q1 {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private final Text word = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] lineAsArray = line.split("\\::");
			word.set(lineAsArray[0]);
			context.write(word, one);
		}
	}

	public static class UserIdRatingsWrapper implements
			Comparable<UserIdRatingsWrapper> {

		public String userID;
		public Double ratingCount;

		public UserIdRatingsWrapper(String userId, Double ratingsCount) {
			userID = userId;
			ratingCount = ratingsCount;
		}

		@Override
		public int compareTo(UserIdRatingsWrapper obj) {
			// TODO Auto-generated method stub
			return Double.compare(obj.ratingCount, this.ratingCount);
		}

	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		PriorityQueue<UserIdRatingsWrapper> userIdRatingsList = new PriorityQueue<UserIdRatingsWrapper>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			String userId = key.toString().trim();
			Double sum = 0.0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			UserIdRatingsWrapper u = new UserIdRatingsWrapper(userId, sum);
			userIdRatingsList.add(u);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			for (int i = 0; i < 10; i++) {
				UserIdRatingsWrapper u = userIdRatingsList.poll();
				context.write(new Text(u.userID),
						new IntWritable(u.ratingCount.intValue()));
			}
		}
	}

	public static class OutputDataWrapper implements
			Comparable<OutputDataWrapper> {

		public Double ratingCount;
		public String gender;
		public int age;
		public String userId;

		public OutputDataWrapper(String userID, String gender, int age,
				Double rating) {
			this.userId = userID;
			this.gender = gender;
			this.age = age;
			this.ratingCount = rating;
		}

		@Override
		public int compareTo(OutputDataWrapper obj) {
			// TODO Auto-generated method stub
			return Double.compare(obj.ratingCount, this.ratingCount);
		}

	}

	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

		private final HashMap<String, String> userIdRatingsCountMap = new HashMap<String, String>();
		private BufferedReader br;
		HashMap<String, String> userIdLine = new HashMap<String, String>();
		private final ArrayList<OutputDataWrapper> outputList = new ArrayList<OutputDataWrapper>();

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			URI[] cacheFilesLocal = DistributedCache.getCacheFiles(context
					.getConfiguration());
			loadUserIdRatingCountHashMap(cacheFilesLocal[0], context);

		}

		private void loadUserIdRatingCountHashMap(URI cacheFilesLocal,
				Context context) throws IOException {

			String strLineRead = "";
			FSDataInputStream in = null;

			try {

				FileSystem fs = FileSystem.get(cacheFilesLocal,
						context.getConfiguration());
				Path path = new Path(cacheFilesLocal);
				in = fs.open(path);
				br = new BufferedReader(new InputStreamReader(in));
				// Read each line, split and load to HashMap
				while ((strLineRead = br.readLine()) != null) {
					String userIdRatingsArray[] = strLineRead.split("\\t");
					userIdRatingsCountMap.put(userIdRatingsArray[0].trim(),
							userIdRatingsArray[1].trim());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();

			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (br != null) {
					br.close();

				}

			}
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String arrUserAttributes[] = value.toString().split("\\::");

			for (Entry<String, String> entry : userIdRatingsCountMap.entrySet()) {
				String userID = entry.getKey();
				if (userID.equalsIgnoreCase(arrUserAttributes[0].trim())) {

					int age = Integer.parseInt(arrUserAttributes[2].trim());
					String gender = arrUserAttributes[1].trim();
					Double ratingCount = Double.parseDouble(entry.getValue()
							.trim());
					OutputDataWrapper obj = new OutputDataWrapper(userID,
							gender, age, ratingCount);
					outputList.add(obj);
				}
			}

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Collections.sort(outputList);
			for (int i = 0; i < outputList.size(); i++) {
				OutputDataWrapper obj = outputList.get(i);
				Text userID = new Text(obj.userId);
				String userDetailsString = Integer.toString(obj.age) + "\t"
						+ obj.gender + "\t"
						+ Integer.toString(obj.ratingCount.intValue());
				Text userDetailsText = new Text(userDetailsString);
				context.write(userID, userDetailsText);
			}

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "question1");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(HW2Q1.class);
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true);

		Configuration conf1 = new Configuration();
		Job job2 = new Job(conf1, "join");

		job2.setJobName("Map-side join with text lookup file in DCache");
		DistributedCache.addCacheFile((new URI(args[2] + "/part-r-00000")),
				job2.getConfiguration());

		job2.setJarByClass(HW2Q1.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.setMapperClass(Mapper1.class);

		job2.setNumReduceTasks(0);

		job2.waitForCompletion(true);
	}

}