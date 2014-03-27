import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

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

public class HW2Q11 {

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

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		HashMap<String, Integer> userRatingsMap = new HashMap<String, Integer>();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			userRatingsMap.put(key.toString(), sum);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			Integer[] values = userRatingsMap.values().toArray(new Integer[0]);
			Arrays.sort(values, Collections.reverseOrder());
			int maxLength = Collections.max(Arrays.asList(values)).toString()
					.length();
			for (int i = 0; i < 10; i++) {
				for (Entry<String, Integer> entry : userRatingsMap.entrySet()) {

					if (values[i].equals(entry.getValue())) {
						String paddedValue = String.format("%0" + maxLength
								+ "d", values[i]);
						Text userId = new Text(entry.getKey());
						IntWritable ratingsCount = new IntWritable(
								Integer.parseInt(paddedValue));
						context.write(userId, ratingsCount);
					}

				}
			}

		}
	}

	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

		private final HashMap<String, String> userIdRatingsCountMap = new HashMap<String, String>();
		private BufferedReader br;
		private String currentUserIdInMap = "";
		HashMap<String, String> userIdLine = new HashMap<String, String>();

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

			if (value.toString().length() > 0) {
				String arrUserAttributes[] = value.toString().split("\\::");

				Iterator it = userIdRatingsCountMap.entrySet().iterator();
				while (it.hasNext()) {
					Entry pairs = (Entry) it.next();
					currentUserIdInMap = (String) pairs.getKey();
					if (arrUserAttributes[0]
							.equalsIgnoreCase(currentUserIdInMap)) {

						userIdLine.put(
								arrUserAttributes[0].toString(),
								(String) pairs.getValue() + "\t"
										+ arrUserAttributes[1].toString()
										+ "\t"
										+ arrUserAttributes[2].toString());
					}
				}

			}

			currentUserIdInMap = "";
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			String[] mapperOutput = userIdLine.values().toArray(new String[0]);
			Arrays.sort(mapperOutput, Collections.reverseOrder());
			for (int i = 0; i < 10; i++) {
				for (Entry<String, String> entry : userIdLine.entrySet()) {

					if (mapperOutput[i].equals(entry.getValue())) {

						String[] splitLine = entry.getValue().split("\\t");
						String result = "";
						for (int j = splitLine.length - 1; j >= 0; j--) {
							result += (splitLine[j] + "\t");
						}
						Text userId = new Text(entry.getKey());

						Text line = new Text(result);
						context.write(userId, line);
					}

				}
			}

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = new Job(conf, "question1");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(HW2Q11.class);
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

		job2.setJarByClass(HW2Q11.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.setMapperClass(Mapper1.class);

		job2.setNumReduceTasks(0);

		job2.waitForCompletion(true);
	}
}