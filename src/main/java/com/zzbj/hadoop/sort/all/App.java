package com.zzbj.hadoop.sort.all;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/***
 * mapreduce App
 * 
 * @author zhuhuijun
 *
 */
public class App {

	public static void main(String[] args) throws Exception {

		String[] aa = new String[2];
		aa[0] = "/user/ncdc/1901.gz";
		aa[1] = "/user/out10";
		if (args.length == 2) {
			aa[0] = args[0];
			aa[1] = args[1];
		}
		if (aa.length != 2) {
			System.err.println("Usage: max temp <inputpath> <outputpath>");
			System.exit(-1);
		}
//		System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.2");
		Job job = Job.getInstance();
		Configuration configuration = job.getConfiguration();

		FileSystem fs = FileSystem.get(configuration);
		fs.delete(new Path(args[1]), true);
		job.setJarByClass(App.class);

		job.setJobName("Max Temperature");// 设置作业名称的
		FileInputFormat.addInputPath(job, new Path(aa[0])); // 设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(aa[1])); // 设置输出路径
		
		
		FileInputFormat.setMaxInputSplitSize(job, 1024*1024*128);
		FileInputFormat.setMinInputSplitSize(job, 1024*1024*128);
		
		//设置分区函数用于全排序的
		job.setPartitionerClass(AllSortPartitional.class);
		

		job.setMapperClass(MaxTempMap.class);
		job.setReducerClass(MaxTempReducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		//job.setCombinerClass(MaxTempReducer.class);
		
		job.setNumReduceTasks(3);//设置reduce个数
		
		System.out.println(configuration.get("fs.defaultFS"));
		System.out.println("over!!!!!!");
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
