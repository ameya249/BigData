import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class HW2Q2Not {
	public static ArrayList<String> maleUserList = new ArrayList<String>();
	public static HashMap<String, String> moviesWithReqGenres = new HashMap<String, String>();
	public static HashMap<String, Double> movieAvgs = new HashMap<String, Double>();
	public static HashMap<String, String> moviesWithRequiredAvgsAndGenres = new HashMap<String, String>();
	public static HashMap<String, String> moviesMaleUserRatersMap = new HashMap<String, String>();

	public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] lineAsArray = line.split("\\::");
			if (lineAsArray[1].trim().equalsIgnoreCase("M")) {
				maleUserList.add(lineAsArray[0]);
				context.write(new Text("male"), new Text(maleUserList.size()
						+ ""));
			}

		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] lineAsArray = line.split("\\::");
			if (lineAsArray[2].trim().contains("Action")
					|| lineAsArray[2].trim().contains("Drama")) {
				moviesWithReqGenres.put(lineAsArray[0], lineAsArray[1] + "::"
						+ lineAsArray[2]);
				context.write(new Text(lineAsArray[0]), new Text(lineAsArray[1]
						+ "::" + moviesWithReqGenres.size() + ""));
			}
		}
	}

	public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] lineAsArray = line.split("\\::");

			String userIdToAdd = "";
			if (maleUserList.contains(lineAsArray[0])) {
				userIdToAdd = lineAsArray[0];
				if (!moviesMaleUserRatersMap.containsKey(lineAsArray[1]))
					moviesMaleUserRatersMap.put(lineAsArray[1], userIdToAdd);
				else {
					String existingUserId = moviesMaleUserRatersMap
							.get(lineAsArray[1]);
					moviesMaleUserRatersMap.remove(lineAsArray[1]);
					existingUserId = existingUserId + "::" + userIdToAdd;
					moviesMaleUserRatersMap.put(lineAsArray[1], existingUserId);

				}
			}
			context.write(new Text(lineAsArray[1]), new Text(lineAsArray[2]));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public static ArrayList<String> localMaleUserList = maleUserList;

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			int count = 0;
			double doubleAvg = 0.0;
			for (Text val : values) {
				// String[] valueArray = val.toString().split("\\::");
				sum += Integer.parseInt(val.toString());
				count++;
			}
			doubleAvg = (double) sum / count;
			movieAvgs.put(key.toString(), doubleAvg);
			// context.write(new Text(key.toString()), new Text(doubleAvg +
			// ""));

		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			for (Entry<String, Double> entry : movieAvgs.entrySet()) {
				if (entry.getValue() >= 4.4 || entry.getValue() <= 4.7) {
					String toInsert = entry.getKey();
					moviesWithRequiredAvgsAndGenres.put(toInsert, entry
							.getValue().toString());
				}
			}

			for (Entry entry : moviesWithRequiredAvgsAndGenres.entrySet()) {
				if (moviesMaleUserRatersMap.containsKey(entry.getKey())) {
					String movieId = (String) entry.getKey();
					String movieDetails = moviesWithReqGenres.get(entry
							.getKey());
					String[] movieAttributes = movieDetails.split("\\::");
					String movieName = movieAttributes[0];
					String movieGenre = movieAttributes[1];
					String avgRating = (String) entry.getValue();
					context.write(new Text(movieId), new Text(movieName + "\t"
							+ movieGenre + "\t" + avgRating));
				}
			}
			context.write(new Text(maleUserList.size() + ""), new Text(
					moviesWithRequiredAvgsAndGenres.size() + "" + "\t"
							+ moviesWithReqGenres.size() + "" + ""
							+ localMaleUserList.size() + ""));

		}

		/*
		 * public void constructJoin(Context context) throws IOException,
		 * InterruptedException { // TODO Auto-generated method stub for (Entry
		 * entry : moviesWithRequiredAvgsAndGenres.entrySet()) { if
		 * (moviesMaleUserRatersMap.containsKey(entry.getKey())) { String
		 * movieId = (String) entry.getKey(); String movieDetails =
		 * moviesWithReqGenres.get(entry .getKey()); String[] movieAttributes =
		 * movieDetails.split("\\::"); String movieName = movieAttributes[0];
		 * String movieGenre = movieAttributes[1]; String avgRating = (String)
		 * entry.getValue(); context.write(new Text(movieId), new Text(movieName
		 * + "\t" + movieGenre + "\t" + avgRating)); } } }
		 */

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration config = new Configuration();
		try {
			Job job1 = new Job(config, "users");
			job1.setOutputKeyClass(Text.class);
			job1.setOutputValueClass(Text.class);
			job1.setJarByClass(HW2Q2.class);

			// MultipleInputs.addInputPath(job1, new Path(args[0]),
			// TextInputFormat.class, Map1.class);
			// MultipleInputs.addInputPath(job1, new Path(args[1]),
			// TextInputFormat.class, Map2.class);
			// MultipleInputs.addInputPath(job1, new Path(args[2]),
			// TextInputFormat.class, Map3.class);

			// job1.setCombinerClass(Reduce.class);
			// job1.setReducerClass(Reduce.class);
			job1.setMapperClass(Map1.class);

			job1.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(job1, new Path(args[0]));
			FileOutputFormat.setOutputPath(job1, new Path(args[3]));
			job1.setNumReduceTasks(0);
			job1.waitForCompletion(true);

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
		Configuration config1 = new Configuration();
		Job job2 = null;
		try {
			job2 = new Job(config1, "movies");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setJarByClass(HW2Q2.class);

		job2.setMapperClass(Map2.class);

		job2.setOutputFormatClass(TextOutputFormat.class);
		try {
			FileInputFormat.addInputPath(job2, new Path(args[1]));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job2, new Path(args[4]));
		job2.setNumReduceTasks(0);
		try {
			job2.waitForCompletion(true);
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
		Configuration config2 = new Configuration();

		Job job3 = null;
		try {
			job3 = new Job(config2, "join");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		job3.setJarByClass(HW2Q2.class);

		job3.setMapperClass(Map3.class);
		// job3.setCombinerClass(Reduce.class);
		job3.setReducerClass(Reduce.class);
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setOutputFormatClass(TextOutputFormat.class);
		try {
			FileInputFormat.addInputPath(job3, new Path(args[2]));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		FileOutputFormat.setOutputPath(job3, new Path(args[5]));

		try {
			job3.waitForCompletion(true);
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
