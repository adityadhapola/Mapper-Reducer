package edu.rmit.cosc2367.s3788649.BigDataAssignment;

import org.apache.log4j.Level;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class TaskOne {
	//Main Function
	public static void main(String [] args) throws Exception
	{
		Configuration c= new Configuration();
		String[] files= new GenericOptionsParser(c,args).getRemainingArgs();
		Path input= new Path(files[0]);
		Path output= new Path(files[1]);
		Job j=new Job(c,"TaskOneMapReduce");
		j.setJarByClass(TaskOne.class);
		j.setMapperClass(WordTypeCountMapper.class);
		j.setCombinerClass(WordTypeCountReducer.class);
		j.setPartitionerClass(WordTypeCountPartitioner.class); 
		j.setReducerClass(WordTypeCountReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}
	
	
	// Logic
	public static String getWordType(String word){
		 		
		int l = word.length();
		if(l>=1 && l<=4){
			return "short"; 		//short words (1-4 letters)
		}else if(l>=5 && l<=7){
			return "medium";  		//medium words (5-7 letters)
		}else if(l>=8 && l<=10){
			return "long";			//long words (8-10 letters)
		}else{
			return "extralong";		//extra-long words (More than 10 letters)
		}

	}
	
	
	// Mapper
	public static class WordTypeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private static final Logger LOG = Logger.getLogger(TaskOne.class);
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The Mapper task of Aditya Ravindra Dhapola, s3788649");
			String line = value.toString();
			//String[] words=line.split("\\s+");
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				String wordType = getWordType(word);
				Text outputKey = new Text(wordType);
				IntWritable outputValue = new IntWritable(1);
				con.write(outputKey, outputValue);
		   }
		}
	}
	
	
	// Partitioner
	public static class WordTypeCountPartitioner extends Partitioner<Text, IntWritable>
	{
		public int getPartition(Text sizeType, IntWritable intWritable, int numReduceTasks) 
	    {
			String sType = sizeType.toString();
			if(sType.equalsIgnoreCase("short") || sType.equalsIgnoreCase("medium")){
				return 0;
			}else if(sType.equalsIgnoreCase("long")){
				return 1%numReduceTasks;
			}else{
				return 2%numReduceTasks;
			}
	    }
	}

	// Reducer
	public static class WordTypeCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private static final Logger LOG = Logger.getLogger(TaskOne.class);
		public void reduce(Text word, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The Reducer task of Aditya Ravindra Dhapola, s3788649");
			int sum = 0;
			for(IntWritable value : values)
			{
				sum += value.get();
			}
			con.write(word, new IntWritable(sum));
		}

	}

}
