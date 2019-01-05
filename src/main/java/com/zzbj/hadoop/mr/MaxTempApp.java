package com.zzbj.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
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
public class MaxTempApp {

	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: max temp <inputpath> <outputpath>");
			System.exit(-1);
		}
//		System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.2");
		Job job = Job.getInstance();
		job.setJarByClass(MaxTempApp.class);
		job.setJobName("Max Temperature");// 设置作业名称的
		FileInputFormat.addInputPath(job, new Path(args[0])); // 设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // 设置输出路径
		job.setMapperClass(MaxTempMap.class);
		job.setReducerClass(MaxTempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Configuration configuration = job.getConfiguration();
		System.out.println(configuration.get("fs.defaultFS"));
		System.out.println("over!!!!!!");
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
