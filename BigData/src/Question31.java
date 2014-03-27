


import java.io.IOException;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Question31 {
        
 public static class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private DoubleWritable age = new DoubleWritable();
    private Text zip = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lineAsArray = line.split("\\::");
        zip.set(lineAsArray[4]);
        age.set(Double.parseDouble(lineAsArray[2]));
        context.write(zip, age);
       }
 } 
        
 public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
      throws IOException, InterruptedException {
    	
        double sum = 0.0;
        int count = 0; 
        double doubleAvg = 0.0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
        doubleAvg = (double) (sum/count);
        DoubleWritable avg = new DoubleWritable(doubleAvg);
        context.write(key, avg);
    }
    
  }
 
 public static class Map1 extends Mapper<LongWritable, Text, Text,DoubleWritable> {
     private DoubleWritable avg = new DoubleWritable();
     private Text zip = new Text();
         
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         String[] lineAsArray = line.split("\t");
         zip.set(lineAsArray[0]);
         avg.set(Double.parseDouble(lineAsArray[1]));
         context.write(zip, avg);
        }
  } 
         
  public static class Reduce1 extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	 TreeMap <String,Double> zipAges = new TreeMap<String,Double>();
	 
	 
	 

     public void reduce(Text key, Iterable<DoubleWritable> values, Context context) 
       throws IOException, InterruptedException {
    	   
    	   
    	  for(DoubleWritable value : values)
    	  zipAges.put(key.toString(),Double.parseDouble(value.toString()));
    	 }
     
     
     
    @Override
    protected void cleanup(Context context)throws IOException, InterruptedException{
    	 
    	
       if(zipAges.size()==0)
    	   throw new IOException("zipAges is empty");
       Double[] values = (Double[])zipAges.values().toArray(new Double[0]);
       Arrays.sort(values);
       String[] keys = (String[])zipAges.keySet().toArray(new String[0]);
       
       if(values.length==0 || keys.length==0 )
    	   throw new IOException("arrays are empty");
       int count=0;
       for(int i=9;i>=0;i--)
       {
    	   for(Entry<String,Double> entry: zipAges.entrySet()){
    	   if(values[i].equals(entry.getValue()) && count<10)
    	   {
    		       count++;
    			   Text zip = new Text(entry.getKey());
        		   DoubleWritable age= new DoubleWritable(values[i].doubleValue());
        		   context.write(zip,age);
    		   
    		   
    	   }
    	   
       }
       }
    	
    	
}
  }
  
      

   public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();        
    
    Job job = new Job(conf, "question3");
   
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setJarByClass(Question31.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
   boolean isComplete = job.waitForCompletion(true);
    
   if(isComplete)
   {
    Configuration conf2 = new Configuration();        
    Job job2 = new Job(conf2, "question3_2");
   
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(DoubleWritable.class);
    job2.setJarByClass(Question31.class);
    job2.setMapperClass(Map1.class);
    job2.setCombinerClass(Reduce1.class);
    job2.setReducerClass(Reduce1.class);
        
    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        
    job2.waitForCompletion(true);
   }
 }
        
}

