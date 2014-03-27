import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Test {

	public static class GetMaleUsersMap extends
			Mapper<LongWritable, Text, Text, Text> {
		private final Text userId = new Text();
		private final Text gender = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] lineFromUsersFile = value.toString().trim().split("\\::");
			if (lineFromUsersFile[1].equalsIgnoreCase("M")) {
				userId.set(lineFromUsersFile[0].trim());
				gender.set("M" + lineFromUsersFile[0].trim());
			}
			context.write(userId, gender); // userId M

		}
	}

	public static class GetRatingsInfoMap extends
			Mapper<LongWritable, Text, Text, Text> {
		private final Text userId = new Text();
		private final Text ratingDetails = new Text();

		// char toAppend = 'D';

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] lineFromRatingsFile = value.toString().trim()
					.split("\\::");

			userId.set(lineFromRatingsFile[0].trim());
			ratingDetails.set("D" + lineFromRatingsFile[1].trim() + "\t"
					+ lineFromRatingsFile[2].trim());

			context.write(userId, ratingDetails); // userId RMovieId Rating

		}
	}

	public static class MaleUsersRatingsJoinReducer extends
			Reducer<Text, Text, Text, Text> {
		final ArrayList<Text> movieIdRatingList = new ArrayList<Text>();

		final ArrayList<Text> maleUserList = new ArrayList<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			movieIdRatingList.clear();
			maleUserList.clear();

			for (Text val : values) {

				if (val.toString().startsWith("M")) {
					maleUserList.add(new Text(val.toString().substring(1)));

				}/*
				 * else
				 * 
				 * if (val.charAt(0) == 'D') { movieIdRatingList .add(new
				 * Text(val.toString().substring(1))); // MovieId // Rating
				 * 
				 * }
				 */
			}
			if (/* !movieIdRatingList.isEmpty() && */!maleUserList.isEmpty()) {
				for (Text user : maleUserList) {
					// for (Text entry : movieIdRatingList) {
					context.write(key, user); // MaleUserId MovieId Rating
					// }
				}
			}

		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Configuration config = new Configuration();
		Job job1 = null;
		try {
			job1 = new Job(config, "Join1");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		job1.setJarByClass(Test.class);
		job1.setOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		try {
			FileInputFormat.addInputPath(job1, new Path(args[0]));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.setMapperClass(GetMaleUsersMap.class);
		// job1.setMapperClass(GetMaleUsersMap.class);
		job1.setReducerClass(MaleUsersRatingsJoinReducer.class);
		// job1.setNumReduceTasks(0);
		try {
			job1.waitForCompletion(true);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
