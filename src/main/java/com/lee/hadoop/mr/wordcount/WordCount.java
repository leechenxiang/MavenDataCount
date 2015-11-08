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
 * 如何编写MR：
 * 1.分析具有的业务逻辑，确定输入输出数据的类型
 * 2.自定义一个类，这个类继承Mapper类，重写map()方法, 实现具体业务逻辑，将新的key，value输出
 * 3.自定义一个类，这个类继承Reducer类，重写reduce()方法
 * 4.将自定义的mapper和reducer通过Job对象组装起来
 * 
 * @author ibm
 *
 */
public class WordCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 构建Job对象
		Job job = Job.getInstance(new Configuration());
		
		// *注意：main方法所在类
		job.setJarByClass(WordCount.class);
		
		// 组装 mapper和reducer
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		// 设置HDFS读取路径
		FileInputFormat.setInputPaths(job, new Path("hdfs://centos01:9000/words"));
		
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// 设置HDFS输出路径
		FileOutputFormat.setOutputPath(job, new Path("hdfs://centos01:9000/wcout_combiner"));
		
		// Combiner是一个特殊的reducer, 在map端小计
		// 在此把WCReducer当做是一个combiner
		job.setCombinerClass(WCReducer.class);
		
		
		// true：执行过程中打印进度和详情
		job.waitForCompletion(true);
	}

	
}
