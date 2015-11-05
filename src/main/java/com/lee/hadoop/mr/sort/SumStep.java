package com.lee.hadoop.mr.sort;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SumStep {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(SumStep.class);
		
		job.setMapperClass(SumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		FileInputFormat.setInputPaths(job, new Path("/amount_sort.txt"));
		
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		FileOutputFormat.setOutputPath(job, new Path("/amount_sort_results"));
		
		job.waitForCompletion(true);
		
	}

	public static class SumMapper extends Mapper<LongWritable, Text, Text, InfoBean> {

		// 只new一次, 使用set来设值, 比构造函数设值有效率
		private Text k = new Text();
		private InfoBean v = new InfoBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			if (fields.length == 0) {
				return;
			}
			String account = fields[0];
			double income = Double.parseDouble(fields[1]);
			double expenses = Double.parseDouble(fields[2]);
			
			k.set(account);
			v.set(account, income, expenses);
			
			context.write(k, v);
		}
		
	}
	
	public static class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean> {

		private InfoBean v = new InfoBean();
		
		@Override
		protected void reduce(Text key2, Iterable<InfoBean> val2s, Context context) throws IOException, InterruptedException {

			double incomes = 0;
			double expensess = 0;
			
			for (InfoBean info : val2s) {
				incomes += info.getIncome();
				expensess =+ info.getExpenses();
			}
			
			v.set(key2.toString(), incomes, expensess);
			
			context.write(key2, v);
		}
		
	}
}
