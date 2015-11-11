package com.lee.hadoop.mr.ii;

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

public class InverseIndexFinal {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(InverseIndexFinal.class);
		
		job.setMapperClass(IndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://centos01:9000/ii_output/result1"));
		
		job.setReducerClass(IndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://centos01:9000/ii_output/result2"));
		
		job.waitForCompletion(true);
	}
	
	public static class IndexMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text k = new Text();
		private Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] words = line.split("\t");
			
			k.set(words[0]);
			v.set(words[1]);
			context.write(k, v);
		}
		
	}
	
	public static class IndexReducer extends Reducer<Text, Text, Text, Text> {

		private Text k = new Text();
		private Text v = new Text();
		
		@Override
		protected void reduce(Text key2, Iterable<Text> val2s, Context context) throws IOException, InterruptedException {
			
			String str = "";
			for (Text t : val2s) {
				str += t.toString();
			}
			
			k.set(key2);
			v.set(str);
			context.write(k, v);
		}
		
	}
	
}
