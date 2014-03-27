
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map.Entry;
import java.util.TreeMap;

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

public class BigDataHW2Q1 {

	public static class MapToFindUsersRatings extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final Text userID = new Text();
		private final static IntWritable one = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] lineArray = value.toString().trim().split("\\::");
			userID.set(lineArray[0]);
			context.write(userID, one);
		}
	}

	public static class UserIDRatings implements Comparable<UserIDRatings> {

		public String userID;
		public Double totalRating;

		// Default Constructor
		public UserIDRatings() {

		}

		// Constructor to set values
		public UserIDRatings(String uID, Double tRating) {
			userID = uID;
			totalRating = tRating;
		}

		// Override default compare Method to sort objects based on totalRating
		@Override
		public int compareTo(UserIDRatings obj) {
			// TODO Auto-generated method stub
			return Double.compare(obj.totalRating, this.totalRating);
		}

	}

	public static class ReduceToFindTopTenUsers extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		// TreeMap<String, Integer> userRatings = new TreeMap<String,
		// Integer>();
		ArrayList<UserIDRatings> userRatingList = new ArrayList<UserIDRatings>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			String userID = key.toString().trim();
			Double total = 0.0;
			for (IntWritable value : values) {
				total = total + value.get();
			}
			// userRatings.put(userID, total);
			UserIDRatings Obj = new UserIDRatings(userID, total);
			userRatingList.add(Obj);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			Collections.sort(userRatingList);
			for (int i = 0; i < 10; i++) {
				UserIDRatings obj = userRatingList.get(i);
				context.write(new Text(obj.userID), new IntWritable(
						obj.totalRating.intValue()));
			}

		}

	}

	public static class UserDataClass implements Comparable<UserDataClass> {

		public Double rating;
		public String gender;
		public int age;
		public String userID;

		public UserDataClass() {

		}

		public UserDataClass(String userID, String gender, int age,
				Double rating) {
			this.userID = userID;
			this.gender = gender;
			this.age = age;
			this.rating = rating;
		}

		@Override
		public int compareTo(UserDataClass obj) {
			// TODO Auto-generated method stub
			return Double.compare(obj.rating, this.rating);
		}

	}

	public static class MapsideJoin extends
			Mapper<LongWritable, Text, Text, Text> {

		private final TreeMap<String, String> topUserRatingsMap = new TreeMap<String, String>();
		private final TreeMap<String, String> finalDataMap = new TreeMap<String, String>();
		private final ArrayList<UserDataClass> finalArrayList = new ArrayList<UserDataClass>();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] userDataArray = value.toString().trim().split("\\::");
			for (Entry<String, String> entry : topUserRatingsMap.entrySet()) {
				String userID = entry.getKey();
				if (userID.equalsIgnoreCase(userDataArray[0].trim())) {
					// Text outPutKey = new Text(userDataArray[0].trim());
					// String userDataLine =
					// entry.getValue()+"\t"+userDataArray[2]+"\t"+userDataArray[1];
					// finalDataMap.put(userID, userDataLine);
					int age = Integer.parseInt(userDataArray[2].trim());
					String gender = userDataArray[1].trim();
					Double ratingCount = Double.parseDouble(entry.getValue()
							.trim());
					UserDataClass obj = new UserDataClass(userID, gender, age,
							ratingCount);
					finalArrayList.add(obj);
				}

			}

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			/*
			 * String [] userAttributes = (String[])
			 * finalDataMap.values().toArray(new String[0]);
			 * Arrays.sort(userAttributes, Collections.reverseOrder()); for (int
			 * i = 0; i < userAttributes.length; i++) { for (Entry<String,
			 * String> entry: finalDataMap.entrySet()) { String attributesLine =
			 * entry.getValue().trim(); if
			 * (attributesLine.equalsIgnoreCase(userAttributes[i])) { String
			 * mapOutputValue = ""; String [] attributesList =
			 * attributesLine.split("\t"); int length = attributesList.length-1;
			 * while (length >= 0) { mapOutputValue += attributesList[length] +
			 * "\t"; length--; }
			 * 
			 * context.write(new Text(entry.getKey()), new
			 * Text(mapOutputValue)); }
			 * 
			 * } }
			 */

			Collections.sort(finalArrayList);
			for (int i = 0; i < finalArrayList.size(); i++) {
				UserDataClass obj = finalArrayList.get(i);
				Text userID = new Text(obj.userID);
				String userLine = Integer.toString(obj.age) + "\t" + obj.gender
						+ "\t" + Integer.toString(obj.rating.intValue());
				Text userDetails = new Text(userLine);
				context.write(userID, userDetails);
			}

		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			URI[] filesTobeCached = DistributedCache.getCacheFiles(context
					.getConfiguration());
			loadDistributedData(context, filesTobeCached[0]);
			/*
			 * for (Path eachPath : filesTobeCached) { if
			 * (eachPath.getName().toString().trim().equals("part-r-00000")) {
			 * loadDistributedData(context, filesTobeCached[0]); } }
			 */
		}

		private void loadDistributedData(Context context, URI pathToFiles) {
			// String strLine = "";
			// FSDataInputStream inf = null;

			try {

				Path pathOfFile = new Path(pathToFiles);
				FileSystem fs = FileSystem.get(pathToFiles,
						context.getConfiguration());

				FSDataInputStream inf = fs.open(pathOfFile);
				InputStreamReader inSR = new InputStreamReader(inf);
				BufferedReader br = new BufferedReader(inSR);
				String strLine = "";
				while ((strLine = br.readLine()) != null) {
					String[] userRatingsArray = strLine.trim().split("\\t");
					topUserRatingsMap.put(userRatingsArray[0].trim(),
							userRatingsArray[1].trim());
				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Configuration config = new Configuration();
		// config.set("minTop", args[2]);

		try {
			Job job1 = new Job(config, "Part1");
			job1.setOutputValueClass(IntWritable.class);
			job1.setOutputKeyClass(Text.class);

			job1.setJarByClass(BigDataHW2Q1.class);
			job1.setMapperClass(MapToFindUsersRatings.class);
			job1.setCombinerClass(ReduceToFindTopTenUsers.class);
			job1.setReducerClass(ReduceToFindTopTenUsers.class);

			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));
			job1.waitForCompletion(true);

			boolean completionStatus = job1.waitForCompletion(true);
			if (completionStatus) {
				Configuration conf = new Configuration();
				Job job2 = new Job(conf, "MapSideJoin");
				DistributedCache.addCacheFile((new URI(args[2]
						+ "/part-r-00000")), job2.getConfiguration());
				job2.setJarByClass(BigDataHW2Q1.class);
				FileInputFormat.setInputPaths(job2, new Path(args[1]));
				FileOutputFormat.setOutputPath(job2, new Path(args[3]));
				job2.setMapperClass(MapsideJoin.class);

				job2.setNumReduceTasks(0);
				job2.waitForCompletion(true);
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
