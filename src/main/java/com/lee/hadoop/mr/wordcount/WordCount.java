package com.lee.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ��α�дMR��
 * 1.�������е�ҵ���߼���ȷ������������ݵ�����
 * 2.�Զ���һ���࣬�����̳�Mapper�࣬��дmap()����, ʵ�־���ҵ���߼������µ�key��value���
 * 3.�Զ���һ���࣬�����̳�Reducer�࣬��дreduce()����
 * 4.���Զ����mapper��reducerͨ��Job������װ����
 * 
 * @author ibm
 *
 */
public class WordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// ����Job����
		Job job = Job.getInstance(new Configuration());
		
		// *ע�⣺main����������
		job.setJarByClass(WordCount.class);
		
		// ��װ mapper��reducer
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		// ����HDFS��ȡ·��
		FileInputFormat.setInputPaths(job, new Path("hdfs://centos01:9000/words"));
		
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// ����HDFS���·��
		FileOutputFormat.setOutputPath(job, new Path("hdfs://centos01:9000/wcout_combiner"));
		
		// Combiner��һ�������reducer, ��map��С��
		// �ڴ˰�WCReducer������һ��combiner
		job.setCombinerClass(WCReducer.class);
		
		
		// true��ִ�й����д�ӡ���Ⱥ�����
		job.waitForCompletion(true);
	}

	
}
