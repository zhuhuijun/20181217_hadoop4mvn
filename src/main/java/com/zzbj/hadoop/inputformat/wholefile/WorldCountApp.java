package com.zzbj.hadoop.inputformat.wholefile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.zzbj.hadoop.mr.MaxTempMap;
import com.zzbj.hadoop.mr.MaxTempReducer;

/***
 * combine file input format only to ctrl big small file <<
 * blocksize
 * 
 * @author zhuhuijun
 *
 */
public class WorldCountApp {

	public static void main(String[] args) throws Exception {

		String[] aa = new String[2];
		aa[0] = "/user/word.txt";
		aa[1] = "/user/out10";
		if (args.length == 2) {
			aa[0] = args[0];
			aa[1] = args[1];
		}
		if (aa.length != 2) {
			System.err.println("Usage: word count <inputpath> <outputpath>");
			System.exit(-1);
		}
		Job job = Job.getInstance();
		job.setJarByClass(WorldCountApp.class);
		job.setJobName("WorldCount");// 设置作业名称的
		FileInputFormat.addInputPath(job, new Path(aa[0])); // 设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(aa[1])); // 设置输出路径

		job.setInputFormatClass(WholeInputFormat.class); // 设置输入的格式类

		FileInputFormat.setMaxInputSplitSize(job, 30);
		FileInputFormat.setMinInputSplitSize(job, 30);

		job.setMapperClass(WorldCountMap.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Configuration configuration = job.getConfiguration();
		System.out.println(configuration.get("fs.defaultFS"));
		System.out.println("over!!!!!!");
		// 设置reduce的数量
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
