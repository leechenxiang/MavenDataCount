package com.lee.hadoop.mr.sort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortStep {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config);
		
		job.setJarByClass(SortStep.class);
		
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(InfoBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path("hdfs://centos01:9000/amount_sort_step1_results"));
		
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://centos01:9000/amount_sort_step2_results"));
		
		job.waitForCompletion(true);
	}

	// �����key2���е�, ����key2λ����infoBean
	// ����Ҫ���, ����NullWritable
	public static class SortMapper extends Mapper<LongWritable, Text, InfoBean, NullWritable> {

		private InfoBean k = new InfoBean();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");

			String account = fields[0];
			double income = Double.parseDouble(fields[1]);
			double out = Double.parseDouble(fields[2]);
			
			k.set(account, income, out);
			
			context.write(k, NullWritable.get());
		}
	}
	
	public static class SortReducer extends Reducer<InfoBean, NullWritable, Text, InfoBean> {

		private Text k = new Text();
		
		// bean��keyλ�û�ʹ��hadoop��Ĭ������
		// ֻҪ����Ҫ������Ǹ�bean����k2λ�þ���������
		// compareTo���������MR�Լ����õ�, �����shuffle��ʱ����õ�
		@Override
		protected void reduce(InfoBean bean, Iterable<NullWritable> value, Context context) throws IOException, InterruptedException {
			String account = bean.getAccount();
			k.set(account);
			context.write(k, bean);
		}
		
	}
	
}
