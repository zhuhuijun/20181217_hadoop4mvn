package com.zzbj.hadoop.wc2;

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
 * mapreduce App
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
//		System.setProperty("hadoop.home.dir", "D:\\bigdata\\hadoop-2.7.2");
		Job job = Job.getInstance();
		// mapreduce.job.jar
		job.setJarByClass(WorldCountApp.class);
		// mapreduce.job.name
		job.setJobName("WorldCount");// 设置作业名称的
		// mapreduce.input.fileinputformat.inputdir
		FileInputFormat.addInputPath(job, new Path(aa[0])); // 设置输入路径
		// mapreduce.output.fileoutputformat.outputdir
		FileOutputFormat.setOutputPath(job, new Path(aa[1])); // 设置输出路径
		// 通过程序设置最小切片和最大切片
//		job.getConfiguration().setInt("mapreduce.input.fileinputformat.split.maxsize", 15);
//		job.getConfiguration().setInt("mapreduce.input.fileinputformat.split.minsize", 10);

		FileInputFormat.setMaxInputSplitSize(job, 30);
		FileInputFormat.setMinInputSplitSize(job, 30);

		job.setMapperClass(WorldCountMap.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		Configuration configuration = job.getConfiguration();
		System.out.println(configuration.get("fs.defaultFS"));
		System.out.println("over!!!!!!");
		//设置reduce的数量
		job.setNumReduceTasks(4);
		//设置分区函数
		job.setPartitionerClass(MyPartitioner.class);
		job.setCombinerClass(WordCountReducer.class);		//设置combiner
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
