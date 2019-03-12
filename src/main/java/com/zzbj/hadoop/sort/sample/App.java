package com.zzbj.hadoop.sort.sample;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/***
 *
 * <p>
 * 通过采样器实现全排序
 * </p>
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

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		// 设置全排序分区函数
		job.setPartitionerClass(TotalOrderPartitioner.class);
		//reduce的数量不能大于Sampler的切片数
		job.setNumReduceTasks(5);
		
		
		//1、key被钻中的概率
		//2、选中的key的个数
		//3、指定最大的切片数量
		InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(
				0.1, 10000, 5);
		InputSampler.writePartitionFile(job, sampler);

		// 添加分区文件到分布式缓存
		String partitionFile = TotalOrderPartitioner.getPartitionFile(configuration);
		URI uri = new URI(partitionFile);
		job.addCacheFile(uri);

		System.out.println(configuration.get("fs.defaultFS"));
		System.out.println("over!!!!!!");
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
