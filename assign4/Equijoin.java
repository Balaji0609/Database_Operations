package Equijoin.cse512;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
	
public class Equijoin 
{
	
	// The mapper class used for mapping purpose.
	public static class MapperClass extends Mapper<LongWritable, Text, LongWritable, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String strtrim = value.toString().replaceAll("\\s",""); // removing all spaces and such things in input.
			// mapperkey parsing which would result in the keys of the set.
			Long keymap = Long.parseLong(strtrim.toString().split(",")[1]); // eg : A,2(key),18000,200{[1] is key}
			//System.out.println(mapperkey);
			//System.out.println(trimmedValue);
			context.write(new LongWritable(keymap), new Text(strtrim)); // writing the key, value to context.
			//System.out.println(strtrim);
		}
	}																																			

	// The reducer class used for reducing purpose.
	public static class ReducerClass extends Reducer<LongWritable, Text, LongWritable, Text> 
	{
	   protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	   {
		   StringBuilder combinedRec = new StringBuilder(); // initializing a string to be empty.
		   // running a loop for each of text value present in the iteratable list.
		   for (Text val : values) 
			{
				combinedRec = combinedRec.append(val.toString()).append(":"); // combining each record into one string with : as a separator.
			}
		   String temp = combinedRec.toString();
		   combinedRec = new StringBuilder(temp.substring(0, combinedRec.length()-1));// removing the last : that will be added
		   //joinedRecord = temp.substring(0, joinedRecord.length()-1);
		   temp = combinedRec.toString();
		   String[] recArray = temp.split(":"); // storing all the split records into a string array.
		   
		   // iterating the array and checking if the key matches and then combining them both.
		   for(int i=0; i<recArray.length-1; i++)
		   {
			   for(int j=i+1; j<recArray.length; j++)
			   {
				   if(!recArray[i].split(",")[0].equals(recArray[j].split(",")[0]))
				   {
					   combinedRec = new StringBuilder(recArray[i]); // creating string builder with 1st record.
					   combinedRec.append(",").append(recArray[j]);  // adding second record to the string builder.
					   context.write(null, new Text(combinedRec.toString()));
				   }
			   }
		   }
		}
	}
	

	// This function is used for initializing with job related variables.
	public static void initFunc(Job job)
	{
		job.setJarByClass(Equijoin.class);
	    job.setMapperClass(MapperClass.class);
	    job.setReducerClass(ReducerClass.class);	    
	    job.setOutputKeyClass(LongWritable.class);
	    job.setOutputValueClass(Text.class);
	    
		
	}
	public static void main(String[] args) throws Exception 
	{
	    Configuration config = new Configuration(); // creating a new configuration for assigning to a job.
	    Job job = Job.getInstance(config, "EquiJoin"); // creating a job with the config and class name.
	    //calling the initfunc to initialize the job var.
	    Equijoin.initFunc(job);
	    // file formatting based on the job and args describing the file location.
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    // exit the system if the map reduce job is complete.
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	 }
}