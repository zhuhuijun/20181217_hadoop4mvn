package com.zzbj.hadoop.sort.secondary;

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
		if (args.length != 2) {
			System.err.println("Usage: max temp <inputpath> <outputpath>");
			System.exit(-1);
		}

		Job job = Job.getInstance();
		Configuration configuration = job.getConfiguration();

		FileSystem fs = FileSystem.get(configuration);
		fs.delete(new Path(args[1]), true);
		job.setJarByClass(App.class);

		job.setJobName("Max Temperature");// 设置作业名称的
		FileInputFormat.addInputPath(job, new Path(args[0])); // 设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1])); // 设置输出路径

		job.setMapperClass(MaxTempMap.class);
		job.setReducerClass(MaxTempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.out.println(configuration.get("fs.defaultFS"));
		System.out.println("over!!!!!!");
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
