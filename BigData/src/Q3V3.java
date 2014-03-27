

import java.io.IOException;     
import java.util.TreeMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

        
public class Q3V3 {
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
        
 public static class Map2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
   public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] lineFromFile = value.toString().split("\t");
	        Double avgAge = Double.parseDouble(lineFromFile[1].toString()); 
	        context.write(new Text(lineFromFile[0]), new DoubleWritable(avgAge));
	        
	   }
 }

 public static class Reduce2 extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	 	TreeMap<String, Double> zipMap = new TreeMap<String, Double>();
        
	   public void reduce(Text key, DoubleWritable value, Context context) 
	      throws IOException, InterruptedException {
	    	String zipCode = key.toString().trim();
	    	Double avgAge = Double.parseDouble((value.toString()));
	        //zipMap.put(zipCode, avgAge);
	        context.write(new Text("10"), new DoubleWritable(11.0));
	   }
	    
/*	   protected void cleanup(Context context)throws IOException, InterruptedException{
		// TODO Auto-generated method stub		   

		   Iterator<Entry<String, Double>> itr = sortedSet.iterator();	   		   
			for(int i = 0 ; i < 10 && itr.hasNext(); i++){
				Entry<String, Double> entry = itr.next();
				context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
			}
			
			Iterator<Entry<String, Double>> itr = zipMap.descendingMap().entrySet().iterator();
	    	for (int i = 1; i <= 10 && itr.hasNext();i++) {
	    		//Entry<String, Double> entry = itr.next();
	    		if (i==10) {
	    			//context.write(new Text("10"), new DoubleWritable(11.0));
				}
	    			
			}
		   
	   }*/	    
	    
 }
 
 public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();        
    Job job = new Job(conf, "ZipCodes");
    job.setOutputKeyClass(Text.class);
    //job.setMapOutputValueClass(IntWritable.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setJarByClass(Q3V3.class);
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
    	conf2.set("numTop", args[3]);
        Job job2 = new Job(conf2, "TopTenCodes");
        job2.setOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setJarByClass(Q3V3.class);
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
