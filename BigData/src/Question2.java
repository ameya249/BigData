

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class Question2 {
        
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    
    private Text genre = new Text();
    
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        Configuration conf = context.getConfiguration();
        String movieList = conf.get("listOfMovies");
        
        String[] movieArray = movieList.split("\\|");
        String[] lineAsArray = line.split("\\::");
        String movieName = lineAsArray[1].replaceAll("[\\W()]+\\(\\d+\\)","").trim();

        
        for(String inputMovie : movieArray)
        {
         if(inputMovie.trim().equalsIgnoreCase(movieName))
         { 
        	 String[] genreArray = lineAsArray[2].split("\\|");
        	 Text movie = new Text();
        	 movie.set(movieName);
        	 for(String currentGenre : genreArray)
        	 {
                genre.set(currentGenre);
                context.write(genre, movie);
        	 }
         }
       }
        
 } 
}
        
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
    	Text movieText = new Text("");
    	context.write(key, movieText);
    }
 }
        
 public static void main(String[] args) throws Exception {
	 
    Configuration conf = new Configuration();        
    conf.set("listOfMovies", args[2]);
    
    Job job = new Job(conf, "question2");
   
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setJarByClass(Question2.class);
    job.setMapperClass(Map.class);
    job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    job.waitForCompletion(true);
 }
        
}