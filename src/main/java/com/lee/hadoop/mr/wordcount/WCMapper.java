package com.lee.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Mapper<> ���������������������
// Ϊɶ��LongWritable����Long�أ�ԭ����Hadoop�����л����ƱȽϿ죬jdk�Դ������л����ƺܺã����Ǻ���Ч�ʲ���
// key ������ַ���ƫ����
// Text ��Ӧ JDK�� String
// Mapper<����1(�ַ���ƫ����), ����1, ���2, ���2>
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// ��������V1
		String line = value.toString();
		// �з�����
		String[] words = line.split(" ");
		// ѭ��
		for (String s : words) {
			// ����һ��, ��¼һ��, ���
			context.write(new Text(s), new LongWritable(1));
		}
	}
	
}
