package com.zzbj.hadoop.inputformat.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/***
 * combine file input format only to ctrl big small file <<
 * blocksize
 *
 * @author zhuhuijun
 *
 */
public class MyDBApp {

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
		job.setJarByClass(MyDBApp.class);
		job.setJobName("DBImport");// 设置作业名称的

		Configuration configuration = job.getConfiguration();
		FileSystem fs = FileSystem.get(configuration);
		fs.delete(new Path(args[1]), true);
		System.out.println(configuration.get("fs.defaultFS"));
		System.out.println("over!!!!!!");

		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径
		job.setInputFormatClass(DBInputFormat.class); // 设置数据库输入格式
		DBConfiguration.configureDB(configuration, "com.mysql.jdbc.Driver",
				"jdbc:mysql://192.168.58.99:3306/mytest", "zhuhj", "zzbj891016");// 设置数据库配置
		DBInputFormat.setInput(job, HadoopUserDBWritable.class,
				"select id,name,age from hadoopusers", "select count(*) from hadoopusers");
		// 设置mapper
		job.setMapperClass(DBMapper.class);
		// 设置输出格式
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		// reduce 0
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}

}
