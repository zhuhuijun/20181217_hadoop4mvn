package com.zzbj.hadoop.outputformat.seq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/***
 * combine file input format only to ctrl big small file <<
 * blocksize
 * 
 * @author zhuhuijun
 *
 */
public class App {

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

		Configuration configuration = job.getConfiguration();
		FileSystem fs = FileSystem.get(configuration);
		fs.delete(new Path(args[1]), true);

		job.setJarByClass(App.class);
		job.setJobName("WorldCount");// 设置作业名称的

		FileInputFormat.addInputPath(job, new Path(aa[0])); // 设置输入路径
		FileOutputFormat.setOutputPath(job, new Path(aa[1])); // 设置输出路径

		job.setOutputFormatClass(SequenceFileOutputFormat.class);// 设置输出格式类
		SequenceFileOutputFormat.setCompressOutput(job, true);
		SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

		job.setMapperClass(WorldCountMap.class);
		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 设置reduce的数量
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
