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
		// 如果 k2 v2 与 k3 v3 一一对应, 则下面2行可以省略; 即 reducer<> 的泛型
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(DataBean.class);
		
		// hdfs路径从args数组接受
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
		
		// 如果不设置，那么还是会把数据分析后放在同一个文件中, 分区就没起作用
		job.setNumReduceTasks(2);
		// 设置分区
		job.setPartitionerClass(carrierOperatorPartitioner.class);
		
		
		// 在linux上运行时可以debug的; 本地模式
		// hadoop三种模式: 
		// 	 本地模式：代码在此写完可以直接运行, 只启一个mapper和一个reducer
		//  jar包模式, 提交到集群或者伪分布式
		job.waitForCompletion(true);
	}
	
	/**
	 * 在mapper执行完之后
	 * reducer执行前
	 * 泛型对于mapper的输出 K2 V2
	 */
	public static class carrierOperatorPartitioner extends Partitioner<Text, DataBean> {

		private static Map<String, Integer> dataSourceMap = new HashMap<String, Integer>();
		static {
//			dataSourceMap.put("131", 2);	// 中国联通
//			dataSourceMap.put("132", 2);	// 中国联通
//			dataSourceMap.put("133", 2);	// 中国联通
//			dataSourceMap.put("134", 2);	// 中国联通
//			                                  
//			dataSourceMap.put("150", 2);	// 中国联通
//			dataSourceMap.put("151", 2);	// 中国联通
//			dataSourceMap.put("152", 2);	// 中国联通
//			dataSourceMap.put("153", 2);	// 中国联通
//			dataSourceMap.put("154", 2);	// 中国联通
//			dataSourceMap.put("155", 2);	// 中国联通
//			dataSourceMap.put("156", 2);	// 中国联通
//			dataSourceMap.put("157", 2);	// 中国联通
//			dataSourceMap.put("158", 2);	// 中国联通
//			dataSourceMap.put("159", 2);	// 中国联通
//			
//			dataSourceMap.put("135", 1);	// 中国移动
//			dataSourceMap.put("136", 1);	// 中国移动
//			dataSourceMap.put("137", 1);	// 中国移动
//			dataSourceMap.put("138", 1);	// 中国移动
//			dataSourceMap.put("139", 1);	// 中国移动
//			dataSourceMap.put("138", 1);	// 中国移动
//			dataSourceMap.put("138", 1);	// 中国移动
//			dataSourceMap.put("138", 1);	// 中国移动
//			dataSourceMap.put("138", 1);	// 中国移动
//			dataSourceMap.put("138", 1);	// 中国移动
//			dataSourceMap.put("138", 1);	// 中国移动
//			dataSourceMap.put("138", 1);	// 中国移动
//			
//			dataSourceMap.put("180", 3);	// 中国电信
//			dataSourceMap.put("181", 3);	// 中国电信
//			dataSourceMap.put("182", 3);	// 中国电信
//			dataSourceMap.put("183", 3);	// 中国电信
//			dataSourceMap.put("184", 3);	// 中国电信
//			dataSourceMap.put("185", 3);	// 中国电信
//			dataSourceMap.put("186", 3);	// 中国电信
//			dataSourceMap.put("187", 3);	// 中国电信
//			dataSourceMap.put("188", 3);	// 中国电信
//			dataSourceMap.put("189", 3);	// 中国电信
			
			dataSourceMap.put("139", 1);
			dataSourceMap.put("137", 1);
		}
		
		
		/**
		 * int 是分区号
		 * K2 V2
		 * numPartitions 又reducer的数量来决定
		 */
		@Override
		public int getPartition(Text key, DataBean value, int numPartitions) {
			
			String mobile = key.toString();
			String subMobile = mobile.substring(0, 3);
			
			// 此处一般需要对接webservice, 或者查询数据库, 但是由于效率太低, 可以把数据放入缓存或者预先读取放入键值对中
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
