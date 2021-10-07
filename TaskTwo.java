package edu.rmit.cosc2367.s3788649.BigDataAssignment;

import org.apache.log4j.Level;

import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//In-Mapper Combining (without preserving state across documents)

public class TaskTwo {
	public static void main(String [] args) throws Exception
	{
		Configuration c= new Configuration();
		String[] files= new GenericOptionsParser(c,args).getRemainingArgs();
		Path input= new Path(files[0]);
		Path output= new Path(files[1]);
		Job j=new Job(c,"TaskThree");
		j.setJarByClass(TaskTwo.class);
		j.setMapperClass(WordCountMapper.class);
		j.setReducerClass(WordCountReducer.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}

	// In-Mapper
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		private static final Logger LOG = Logger.getLogger(TaskTwo.class);
		
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			LOG.setLevel(Level.DEBUG);
			LOG.debug("The mapper task of Aditya Ravindra Dhapola, s3788649");
			HashMap<String, Integer> wordCountMap = new HashMap<String, Integer>();
			String line = value.toString();
			//String[] words=line.split("\\s+");
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				int count = wordCountMap.getOrDefault(word, 0) + 1;
				wordCountMap.put(word, count);
			}
			for(String word: wordCountMap.keySet()) {
				int count = wordCountMap.getOrDefault(word, 0);
				Text outputKey = new Text(word);
				IntWritable outputValue = new IntWritable(count);
				con.write(outputKey, outputValue);
			}
		}
	}
	
	// Reducer
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private static final Logger LOG = Logger.getLogger(TaskTwo.class);
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
