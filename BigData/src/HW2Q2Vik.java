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

public class HW2Q2Vik {

	public static class MapUser extends Mapper<LongWritable, Text, Text, Text> {
		private final Text userID = new Text();
		private final Text userGender = new Text();

		// Map class to get the Users with Gender as 'M'
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				String[] userArray = value.toString().trim().split("::"); // Split
																			// line
																			// from
																			// users.dat
				if (userArray[1].equalsIgnoreCase("M")) {
					userID.set(userArray[0].trim());
					userGender.set("A" + userArray[0].trim());
					context.write(userID, userGender); // Map Output: ("userID",
														// "Gender as M")
				}

			} catch (Exception e) {
				System.out.println("Error in handling users.dat file");
			}
		}
	}

	public static class MapRatings extends
			Mapper<LongWritable, Text, Text, Text> {
		private final Text userID = new Text();
		private final Text ratingsInfo = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] userRatingsArray = value.toString().trim().split("::");

			userID.set(userRatingsArray[0].trim());
			ratingsInfo.set("B" + userRatingsArray[1].trim() + "\t"
					+ userRatingsArray[2].trim());
			context.write(userID, ratingsInfo); // Map Output: ("userID",
												// "movieID	ratings")
		}
	}

	public static class UserRatingsReducer extends
			Reducer<Text, Text, Text, Text> {

		// private Text valueFrom = new Text();
		private final ArrayList<Text> infoUserFile = new ArrayList<Text>();
		private final ArrayList<Text> infoRatingsFile = new ArrayList<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			infoUserFile.clear();
			infoRatingsFile.clear();

			for (Text val : values) {
				if (val.charAt(0) == 'A') {
					infoUserFile.add(new Text(val.toString().substring(1)));

				} else if (val.charAt(0) == 'B') {
					infoRatingsFile.add(new Text(val.toString().substring(1)));

				}

			}
			if (!infoRatingsFile.isEmpty() && !infoUserFile.isEmpty()) {

				for (Text userText : infoUserFile) {
					for (Text ratingText : infoRatingsFile) {
						// context.write(new Text(B.toString().split("\\t")[0]),
						// new Text(A.toString() + "~"
						// +B.toString().split("\\t")[0] +"~"+
						// B.toString().split("\\t")[1]));
						context.write(userText, ratingText);
					}
				}
			}

		}
	}

	public static class InterMediateMap extends
			Mapper<LongWritable, Text, Text, Text> {

		private final Text movieID = new Text();
		private final Text otherInfo = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] interLineArray = value.toString().trim().split("\\t");
			movieID.set(interLineArray[1]);
			// otherInfo.set("*"+interLineArray[2] + "\t" + interLineArray[0]);
			otherInfo.set("*" + interLineArray[2]);
			context.write(movieID, otherInfo); // Mapoutput: (MovieID, Rating)
		}

	}

	public static class MovieMap extends Mapper<LongWritable, Text, Text, Text> {
		private final Text movieID = new Text();
		private final Text movieInfo = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] movieLineArray = value.toString().trim().split("::");

			if (movieLineArray[2].contains("Drama")
					|| movieLineArray[2].contains("Action")) {
				movieID.set(movieLineArray[0]);
				movieInfo.set("$" + movieLineArray[1] + "\t"
						+ movieLineArray[2]);
				context.write(movieID, movieInfo);
			}
		}
	}

	public static class FinalJoinReducer extends
			Reducer<Text, Text, Text, Text> {

		TreeMap<String, String> outputMap = new TreeMap<String, String>();
		ArrayList<Text> movieInFile = new ArrayList<Text>();
		ArrayList<Text> midInfoFile = new ArrayList<Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			midInfoFile.clear();
			movieInFile.clear();
			double count = 0;
			double sum = 0;
			for (Text val : values) {
				if (val.charAt(0) == '$') {
					movieInFile.add(new Text(val.toString().substring(1)));
				} else if (val.charAt(0) == '*') {
					midInfoFile.add(new Text(val.toString().substring(1)));
					sum += Double.parseDouble(val.toString().substring(1)
							.trim());
					count++;
				}
			}

			if (!movieInFile.isEmpty()) {
				double avg = sum / count;
				for (Text ratingsText : midInfoFile) {
					for (Text movieText : movieInFile) {
						if (avg >= 4.4 && avg <= 4.7) {
							String movieDetails = movieText.toString() + "\t"
									+ Double.toString(avg);
							outputMap.put(key.toString(), movieDetails);
						}

					}
				}

			}

			/*
			 * String size1 = Integer.toString(movieInFile.size()); String size2
			 * = Integer.toString(midInfoFile.size()) + ":" +
			 * Integer.toBinaryString(outputMap.size()); //context.write(new
			 * Text(size1), new Text(size2));
			 */
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Entry<String, String> entry : outputMap.entrySet()) {
				context.write(new Text(entry.getKey()),
						new Text(entry.getValue()));
			}
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			Configuration config = new Configuration();
			Job job1;
			job1 = new Job(config, "Part1");
			job1.setJarByClass(HW2Q2Vik.class);
			job1.setOutputValueClass(Text.class);
			job1.setOutputKeyClass(Text.class);

			// job1.setMapperClass(MapUser.class);
			// job1.setMapperClass(MapRatings.class);
			// job1.setCombinerClass(ReduceToFindTopTenUsers.class);
			// job1.setReducerClass(ReduceToFindTopTenUsers.class);

			job1.setInputFormatClass(TextInputFormat.class);
			job1.setOutputFormatClass(TextOutputFormat.class);

			MultipleInputs.addInputPath(job1, new Path(args[0]),
					TextInputFormat.class, MapUser.class);
			MultipleInputs.addInputPath(job1, new Path(args[1]),
					TextInputFormat.class, MapRatings.class);

			// job1.setCombinerClass(UserRatingsReducer.class);
			job1.setReducerClass(UserRatingsReducer.class);
			FileOutputFormat.setOutputPath(job1, new Path(args[2]));
			// job1.waitForCompletion(true);

			// job1.setNumReduceTasks(0);
			// FileInputFormat.addInputPath(job1, new Path(args[0]));
			// FileOutputFormat.setOutputPath(job1, new Path(args[1]));

			job1.waitForCompletion(true);

			boolean completionStatus = job1.waitForCompletion(true);
			if (completionStatus) {
				Configuration conf = new Configuration();
				Job job2 = new Job(conf, "FinalReduceSideJoin");
				job2.setJarByClass(HW2Q2.class);
				job2.setOutputValueClass(Text.class);
				job2.setOutputKeyClass(Text.class);

				job2.setInputFormatClass(TextInputFormat.class);
				job2.setOutputFormatClass(TextOutputFormat.class);

				MultipleInputs.addInputPath(job2, new Path(args[2]),
						TextInputFormat.class, InterMediateMap.class);
				MultipleInputs.addInputPath(job2, new Path(args[3]),
						TextInputFormat.class, MovieMap.class);

				// job2.setCombinerClass(UserRatingsReducer.class);
				job2.setReducerClass(FinalJoinReducer.class);
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
