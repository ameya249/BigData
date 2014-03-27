
import java.io.IOException;     
import java.util.Collections;
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Q31 {
 public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text word = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lineFromFile = value.toString().split("\\::");
        word.set(lineFromFile[4]);
        DoubleWritable ageDouble = new DoubleWritable(Double.parseDouble(lineFromFile[2].trim()));
        context.write(word,ageDouble);

    }
    
 } 
        
 public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
      throws IOException, InterruptedException {

    	double sum = 0;
    	int count = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
       
        double avg = sum/count;
        DoubleWritable avgDouble = new DoubleWritable(avg);
        context.write(key, avgDouble);
    }
 }
        
 public static class Map2 extends Mapper<LongWritable, Text, DoubleWritable, Text> {
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] lineFromFile = value.toString().split("\t");
	        Double avgAge = Double.parseDouble(lineFromFile[1]); 
	        context.write(new DoubleWritable(avgAge), new Text(lineFromFile[0]));
	   }
 }

 public static class Reduce2 extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {
	 	TreeMap<Double, String> zipMap = new TreeMap<Double, String>(Collections.reverseOrder());
        
	    public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
	      throws IOException, InterruptedException {

	    	String conCatZips = "";
	    	Double avgAge = Double.parseDouble(key.toString());
	    	if (!zipMap.containsKey(avgAge)) {
	    		int count = 0;
	    		for (Text zipText : values) {
	    			conCatZips = conCatZips + ", " + zipText.toString();
	    			count++;
		 	    }
	    		if (count==1) {
					conCatZips = conCatZips.substring(0, conCatZips.length()-1);
				}
	    		zipMap.put(avgAge, conCatZips);
			} else {
				conCatZips = zipMap.get(avgAge);
				zipMap.remove(avgAge);
				int count = 0;
				for (Text zipText : values){  
					conCatZips = conCatZips + ", " + zipText.toString();
					
				}
				if (count==1) {
					conCatZips = conCatZips.substring(0, conCatZips.length()-1);
				}
		 	    zipMap.put(avgAge, conCatZips);
			}
	    	context.write(key, new Text(conCatZips));
		}
	    
	    
	    /*protected void cleanup(Context context)throws IOException, InterruptedException{ 
	    	Iterator<Entry<Double, String>> itr = zipMap.entrySet().iterator();
	    	for (int i = 0; i < 10 && itr.hasNext();i++) {
	    		Entry<Double, String> entry = itr.next();
				context.write(new DoubleWritable(entry.getKey()), new Text(entry.getValue()));	
			}
	    }*/
 }

 
 
 public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();        
    Job job = new Job(conf, "ZipCodes");
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setJarByClass(Q31.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    boolean completionStatus = job.waitForCompletion(true);
    
    if (completionStatus) {
    	Configuration conf2 = new Configuration();        
        Job job2 = new Job(conf2, "TopTenCodes");
        job2.setOutputKeyClass(DoubleWritable.class);

        job2.setOutputValueClass(Text.class); job2.setJarByClass(Q31.class);
        job2.setMapperClass(Map2.class);
        job2.setCombinerClass(Reduce2.class);
        job2.setReducerClass(Reduce2.class);
          
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
            
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
        
}
 }
}