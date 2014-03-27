


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
        
public class Question3 {
        
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
 
 public static class Map1 extends Mapper<LongWritable, Text, DoubleWritable,Text> {
     private DoubleWritable avg = new DoubleWritable();
     private Text zip = new Text();
         
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String line = value.toString();
         String[] lineAsArray = line.split("\t");
         zip.set(lineAsArray[0]);
         avg.set(Double.parseDouble(lineAsArray[1]));
         context.write(avg, zip);
        }
  } 
         
  public static class Reduce1 extends Reducer<DoubleWritable,Text,DoubleWritable,Text> {
	 TreeMap <Double,String> zipAges = new TreeMap<Double,String>();
	 
	 
	 

     public void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
       throws IOException, InterruptedException {
    	     String zipToAdd = "";	
    	     Double avgAge = Double.parseDouble(key.toString());
    	     if(!zipAges.containsKey(avgAge))
    	     {
    	     
    	     for(Text zip : values)
    	    	 zipToAdd = zipToAdd+","+zip.toString();
    		 zipAges.put(avgAge,zipToAdd);
    	     }
    	     else
    	     {
    	    	zipToAdd =  zipAges.get(avgAge);
    	    	zipAges.remove(avgAge);
    	    	for(Text value: values)
    	    	{
    	    		zipToAdd = zipToAdd+","+value.toString();
    	    	}
    	    	zipAges.put(avgAge, zipToAdd);
    	     }
    	 }
     
     
     
    @Override
    protected void cleanup(Context context)throws IOException, InterruptedException{
    	 
    	Double[] keys = (Double[]) zipAges.keySet().toArray(new Double[0]);
    	
    	for(int i=9;i>=0;i--)
    	{
    		DoubleWritable age = new DoubleWritable(keys[i].doubleValue());
    		Text zip = new Text(zipAges.get(keys[i]).substring(1));
    		context.write(age, zip);
    	}
    	
    	
}
  }
  
      

   public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();        
    
    Job job = new Job(conf, "question3");
   
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    job.setJarByClass(Question3.class);
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
   
    job2.setOutputKeyClass(DoubleWritable.class);
    job2.setOutputValueClass(Text.class);
    job2.setJarByClass(Question3.class);
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
