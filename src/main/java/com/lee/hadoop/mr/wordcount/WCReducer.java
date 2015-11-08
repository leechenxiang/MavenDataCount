package com.lee.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Reducer<����2, ����2, ���3, ���3>
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key2, Iterable<LongWritable> value2s, Context context) throws IOException, InterruptedException {
		// ��������
		// Text key3 = key2;

		// ����һ��������
		long counts = 0;
		// ѭ��value2s
		for (LongWritable c : value2s) {
			counts += c.get();
		}
		
		// ���
		context.write(key2, new LongWritable(counts));
	}
	
}
