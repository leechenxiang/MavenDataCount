package com.lee.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Reducer<输入2, 输入2, 输出3, 输出3>
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key2, Iterable<LongWritable> value2s, Context context) throws IOException, InterruptedException {
		// 接受数据
		// Text key3 = key2;

		// 定义一个计数器
		long counts = 0;
		// 循环value2s
		for (LongWritable c : value2s) {
			counts += c.get();
		}
		
		// 输出
		context.write(key2, new LongWritable(counts));
	}
	
}
