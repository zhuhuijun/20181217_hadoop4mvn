package com.zzbj.hadoop.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.ArrayFile.Reader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestArrayFile
{

	public static void main(String[] args)
	{
		// TODO Auto-generated method stub

	}
	@SuppressWarnings("deprecation")
	@Test
	public void writeFile() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		 ArrayFile.Writer mWriter = new  ArrayFile.Writer(conf, fs, "hdfs://s100:8020/user/ubuntu/myarrayfile.map",
				Text.class);
		for (int j = 0; j < 1200; j++)
		{
			mWriter.append(new Text("tom"+j));
		}
		mWriter.close();
		System.out.println("over!!!!!!!!!!!!");
	}
	/***
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void readFile() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		 Reader mReader = new ArrayFile.Reader(fs, "hdfs://s100:8020/user/ubuntu/myarrayfile.map", conf);
		// key 需要按照顺序来
		IntWritable key = new IntWritable();
		Text val = new Text();
		while ((val =(Text) mReader.next(val)) != null)
		{
			System.out.println(" value:" + val);
		}
		mReader.get(new IntWritable(22), val);
		mReader.close();

		System.out.println("val:" + val);
		System.out.println("over!!!!!!!!!!!!");
	}
}
