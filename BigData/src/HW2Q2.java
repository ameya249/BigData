import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HW2Q2 {
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

				} else

				if (val.toString().startsWith("D")) {
					movieIdRatingList
							.add(new Text(val.toString().substring(1))); // MovieId
																			// Rating

				}
			}
			if (!movieIdRatingList.isEmpty() && !maleUserList.isEmpty()) {
				for (Text user : maleUserList) {
					for (Text entry : movieIdRatingList) {
						context.write(key, entry); // MaleUserId MovieId Rating
					}
				}
			}

		}

	}

	public static class AvgCalculationHelperMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		private final Text movieId = new Text();
		private final Text rating = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] reducerOutput = value.toString().trim().split("\\t");
			movieId.set(reducerOutput[1]);
			rating.set("A" + reducerOutput[2]);
			context.write(movieId, rating); // movieId ARating
		}

	}

	public static class GetMovieInfoMap extends
			Mapper<LongWritable, Text, Text, Text> {
		private final Text movieId = new Text();
		private final Text movieDetails = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] lineFromMoviesFile = value.toString().trim().split("\\::");
			if (lineFromMoviesFile[2].contains("Drama")
					|| lineFromMoviesFile[2].contains("Action")) {
				movieId.set(lineFromMoviesFile[0]);
				movieDetails.set("B" + lineFromMoviesFile[1] + "\t"
						+ lineFromMoviesFile[2]);
				context.write(movieId, movieDetails); // movieId RTitle Genre
			}

		}

	}

	public static class MovieRatingJoinReducer extends
			Reducer<Text, Text, Text, Text> {
		TreeMap<String, String> finalOutput = new TreeMap<String, String>();
		ArrayList<Text> titleGenreList = new ArrayList<Text>();
		ArrayList<Text> ratingList = new ArrayList<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			titleGenreList.clear();
			ratingList.clear();
			double count = 0;
			double avg = 0.0;
			double sum = 0;
			for (Text val : values) {
				if (val.toString().startsWith("B")) {
					titleGenreList.add(new Text(val.toString().substring(1)));
				} else if (val.toString().startsWith("A")) {
					ratingList.add(new Text(val.toString().substring(1)));
					sum += Double.parseDouble(val.toString().substring(1)
							.trim());
					count++;
				}
			}

			if (!titleGenreList.isEmpty() && !ratingList.isEmpty()) {
				avg = sum / count;
				for (Text ratings : ratingList) {
					for (Text titleAndGenre : titleGenreList) {
						if (avg >= 4.4 && avg <= 4.7) {
							String movieDetails = titleAndGenre.toString()
									+ "\t" + Double.toString(avg);
							finalOutput.put(key.toString(), movieDetails);
						}

					}
				}

			}

		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Entry<String, String> entry : finalOutput.entrySet()) {
				context.write(new Text(entry.getKey()),
						new Text(entry.getValue()));
			}
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			Configuration config = new Configuration();
			Job job1;
			job1 = new Job(config, "Join1");
			job1.setJarByClass(HW2Q2.class);
			job1.setOutputValueClass(Text.class);
			job1.setOutputKeyClass(Text.class);

			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			MultipleInputs.addInputPath(job1, new Path(args[0]),
					TextInputFormat.class, GetMaleUsersMap.class);
			MultipleInputs.addInputPath(job1, new Path(args[1]),
					TextInputFormat.class, GetRatingsInfoMap.class);

			job1.setReducerClass(MaleUsersRatingsJoinReducer.class);
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));

			job1.waitForCompletion(true);

			boolean completionStatus = job1.waitForCompletion(true);
			if (completionStatus) {
				Configuration conf = new Configuration();
				Job job2 = new Job(conf, "Join2");
				job2.setJarByClass(HW2Q2.class);
				job2.setOutputValueClass(Text.class);
				job2.setOutputKeyClass(Text.class);

				job2.setInputFormatClass(TextInputFormat.class);
				job2.setOutputFormatClass(TextOutputFormat.class);

				MultipleInputs
						.addInputPath(job2, new Path(args[2]),
								TextInputFormat.class,
								AvgCalculationHelperMapper.class);
				MultipleInputs.addInputPath(job2, new Path(args[3]),
						TextInputFormat.class, GetMovieInfoMap.class);

				job2.setReducerClass(MovieRatingJoinReducer.class);
				FileOutputFormat.setOutputPath(job2, new Path(args[4]));
				job2.waitForCompletion(true);

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
