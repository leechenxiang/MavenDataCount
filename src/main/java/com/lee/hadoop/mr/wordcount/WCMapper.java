package com.lee.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper<> 泛型限制输入输出的类型
// 为啥用LongWritable不用Long呢，原因是Hadoop的序列化机制比较快，jdk自带的序列化机制很好，但是很慢效率不好
// key 代表的字符的偏移量
// Text 对应 JDK的 String
// Mapper<输入1(字符的偏移量), 输入1, 输出2, 输出2>
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// 接受数据V1
		String line = value.toString();
		// 切分数据
		String[] words = line.split(" ");
		// 循环
		for (String s : words) {
			// 出现一次, 记录一次, 输出
			context.write(new Text(s), new LongWritable(1));
		}
	}
	
}
