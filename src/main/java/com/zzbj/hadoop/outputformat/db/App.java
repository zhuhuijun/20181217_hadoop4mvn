package com.zzbj.hadoop.outputformat.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.zzbj.hadoop.inputformat.db.HadoopUserDBWritable;

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

		job.setOutputFormatClass(DBOutputFormat.class);// 设置输出格式类
		DBOutputFormat.setOutput(job, "wordcount", "word", "num");
		DBConfiguration.configureDB(configuration, "com.mysql.jdbc.Driver",
				"jdbc:mysql://192.168.58.99:3306/mytest", "zhuhj", "zzbj891016");// 设置数据库配置
		
		//map的输入和输出类型
		job.setMapperClass(WorldCountMap.class);
		job.setReducerClass(WordCountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(WordDBWritable.class);
		job.setOutputValueClass(NullWritable.class);

		// 设置reduce的数量
		job.setNumReduceTasks(1);
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
