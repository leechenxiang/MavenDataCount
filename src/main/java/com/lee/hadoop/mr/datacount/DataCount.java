package com.lee.hadoop.mr.datacount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCount {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration cfg = new Configuration();
		Job job = Job.getInstance(cfg);
		
		job.setJarByClass(DataBean.class); 
		
		job.setMapperClass(DCMapper.class);
		// ��� k2 v2 �� k3 v3 һһ��Ӧ, ������2�п���ʡ��; �� reducer<> �ķ���
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DataBean.class);
		
		// hdfs·����args�������
//		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.8.88:9000/HTTP_20130313143750.dat"));
//		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, new Path("hdfs://centos01:9000/input/test.dat"));
		
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		
//		hdfs://192.168.8.88:9000/input/test.dat
//		hdfs://192.168.8.88:9000/output/result
		FileOutputFormat.setOutputPath(job, new Path("hdfs://centos01:9000/output/result"));
//		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// ��������ã���ô���ǻ�����ݷ��������ͬһ���ļ���, ������û������
		// �������ö��reducer, �� >2, ���������������partitioner�ڼ������ļ����ǿյ�
		// ��� <2, �򱨴�
		job.setNumReduceTasks(2);
		// ���÷���
		job.setPartitionerClass(carrierOperatorPartitioner.class);
		
		
		// ��linux������ʱ����debug��; ����ģʽ
		// hadoop����ģʽ: 
		// 	 ����ģʽ�������ڴ�д�����ֱ������, ֻ��һ��mapper��һ��reducer
		//  jar��ģʽ, �ύ����Ⱥ����α�ֲ�ʽ
		job.waitForCompletion(true);
	}
	
	/**
	 * ��mapperִ����֮��
	 * reducerִ��ǰ
	 * ���Ͷ���mapper����� K2 V2
	 */
	public static class carrierOperatorPartitioner extends Partitioner<Text, DataBean> {

		private static Map<String, Integer> dataSourceMap = new HashMap<String, Integer>();
		static {
			dataSourceMap.put("139", 1);
			dataSourceMap.put("137", 1);
		}
		
		/**
		 * int �Ƿ�����
		 * K2 V2
		 * numPartitions ��reducer������������
		 */
		@Override
		public int getPartition(Text key, DataBean value, int numPartitions) {
			String mobile = key.toString();
			String subMobile = mobile.substring(0, 3);
			// �˴�һ����Ҫ�Խ�webservice, ���߲�ѯ���ݿ�, ��������Ч��̫��, ���԰����ݷ��뻺�����Ԥ�ȶ�ȡ�����ֵ����
			Integer code = dataSourceMap.get(subMobile);
			if (code == null) {
				code = 0;
			}
			System.out.println(code);
			return code;
		}
	}
	
	public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean> {

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split("\t");
			String mobile = fields[1];
			// ������ϴ, ������ֱ��return
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
