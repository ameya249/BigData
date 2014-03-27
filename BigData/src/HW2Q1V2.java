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

public class HW2Q1V2 {

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
			/*
			 * String n = conf.get("rating"); int input = Integer.parseInt(n);
			 * if(sum>=input)
			 */
			// context.write(key, new IntWritable(sum));
			userRatingsMap.put(key.toString(), sum);
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

			// Integer[] keys = (Integer[]) userRatingsMap.keySet().toArray(new
			// Integer[0]);
			Integer[] values = userRatingsMap.values().toArray(new Integer[0]);
			Arrays.sort(values, Collections.reverseOrder());
			// int count = 0;
			for (int i = 0; i < 10; i++) {
				for (Entry<String, Integer> entry : userRatingsMap.entrySet()) {

					if (values[i].equals(entry.getValue())) {

						Text userId = new Text(entry.getKey());
						IntWritable ratingsCount = new IntWritable(values[i]);
						context.write(userId, ratingsCount);
					}

				}
			}

		}
	}

	public static class MapperMapSideJoinDCacheTextFile extends
			Mapper<LongWritable, Text, Text, Text> {

		private final HashMap<String, String> DepartmentMap = new HashMap<String, String>();
		private BufferedReader br;
		private String strDeptName = "";
		private final Text txtMapOutputKey = new Text("");
		private final Text txtMapOutputValue = new Text("");

		enum MYCOUNTER {
			RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
		}

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			URI[] cacheFilesLocal = DistributedCache.getCacheFiles(context
					.getConfiguration());

			/*
			 * for (Path eachPath : cacheFilesLocal) { if
			 * (eachPath.getName().toString().trim().equals("part-r-00000")) {
			 * context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
			 * loadDepartmentsHashMap(eachPath, context); } }
			 */

			loadDepartmentsHashMap(cacheFilesLocal[0], context);

		}

		private void loadDepartmentsHashMap(URI cacheFilesLocal, Context context)
				throws IOException {

			String strLineRead = "";
			FSDataInputStream in = null;
			// BufferedReader br = null;

			try {
				// brReader = new BufferedReader(new FileReader(new File(
				// cacheFilesLocal.getPath().toString())));

				FileSystem fs = FileSystem.get(cacheFilesLocal,
						context.getConfiguration());
				Path path = new Path(cacheFilesLocal);
				in = fs.open(path);
				br = new BufferedReader(new InputStreamReader(in));
				// Read each line, split and load to HashMap
				while ((strLineRead = br.readLine()) != null) {
					String deptFieldArray[] = strLineRead.split("\\t");
					DepartmentMap.put(deptFieldArray[0].trim(),
							deptFieldArray[1].trim());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
			} catch (IOException e) {
				context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
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

			context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

			if (value.toString().length() > 0) {
				String arrEmpAttributes[] = value.toString().split("\\::");

				try {
					Iterator it = DepartmentMap.entrySet().iterator();
					while (it.hasNext()) {
						Entry pairs = (Entry) it.next();
						strDeptName = (String) pairs.getKey();
						if (arrEmpAttributes[0].equalsIgnoreCase(strDeptName)) {
							txtMapOutputKey.set(arrEmpAttributes[0].toString());

							txtMapOutputValue.set(arrEmpAttributes[1]
									.toString()
									+ "\t"
									+ arrEmpAttributes[2].toString()
									+ "\t"
									+ (String) pairs.getValue());
							context.write(txtMapOutputKey, txtMapOutputValue);
						}
					}
					// strDeptName = DepartmentMap.get(arrEmpAttributes[0]
					// .toString());
				} finally {
					// strDeptName = ((strDeptName.equals(null) || strDeptName
					// .equals("")) ? "NOT-FOUND" : strDeptName);
				}

				// txtMapOutputKey.set(arrEmpAttributes[0].toString());

				// txtMapOutputValue.set(arrEmpAttributes[1].toString() + "\t"
				// + arrEmpAttributes[2].toString() + "\t"
				/*
				 * + arrEmpAttributes[2].toString() + "\t" +
				 * arrEmpAttributes[3].toString() + "\t" +
				 * arrEmpAttributes[4].toString() + "\t" +
				 * arrEmpAttributes[5].toString() + "\t" +
				 * arrEmpAttributes[6].toString() + "\t"
				 */// + strDeptName);

			}
			// context.write(txtMapOutputKey, txtMapOutputValue);
			strDeptName = "";
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {

		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		// conf.set("rating", args[2]);

		Job job = new Job(conf, "question1");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setJarByClass(HW2Q1V2.class);
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

		job2.setJarByClass(HW2Q1V2.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		job2.setMapperClass(MapperMapSideJoinDCacheTextFile.class);

		job2.setNumReduceTasks(0);

		boolean success = job2.waitForCompletion(true);
	}
}