package com.lee.hadoop.mr.datacount;

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

public class DataCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration cfg = new Configuration();
		Job job = Job.getInstance(cfg);
		
		job.setJarByClass(DataBean.class); 
		
		job.setMapperClass(DCMapper.class);
		// 如果 k2 v2 与 k3 v3 一一对应, 则下面2行可以省略; 即 reducer<> 的泛型
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DataBean.class);
		
		// hdfs路径从args数组接受
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 在linux上运行时可以debug的; 本地模式
		// hadoop三种模式: 
		// 	 本地模式：代码在此写完可以直接运行, 只启一个mapper和一个reducer
		//  jar包模式, 提交到集群或者伪分布式
		job.waitForCompletion(true);
	}
	
	public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String mobile = fields[1];
			// 数据清洗, 脏数据直接return
			long upPayload = 0;
			long downPayload = 0;
			try {
				upPayload = Long.parseLong(fields[8]);
				downPayload = Long.parseLong(fields[9]);
			} catch (NumberFormatException e) {
				return;
			}
			DataBean data = new DataBean(mobile, upPayload, downPayload);
			context.write(new Text(mobile), data);
		}
		
	}
	
	public static class DCReducer extends Reducer<Text, DataBean, Text, DataBean> {

		@Override
		protected void reduce(Text key, Iterable<DataBean> value2s, Context context) throws IOException, InterruptedException {
			long up_sum = 0;
			long down_sum = 0;
			
			for (DataBean data : value2s) {
				up_sum += data.getUpPayload();
				down_sum += data.getDownPayload();
			}
			
			DataBean bean = new DataBean("", up_sum, down_sum);
			context.write(key, bean);
		}
		
	}
	
}
