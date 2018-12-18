package com.zzbj.hadoop.myhadoop;

import org.apache.commons.math3.filter.KalmanFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class TestMapFile
{

	@SuppressWarnings("deprecation")
	@Test
	public void writeFile() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		MapFile.Writer mWriter = new MapFile.Writer(conf, fs, "hdfs://s100:8020/user/ubuntu/mymap.map",
				IntWritable.class, Text.class);
		Text val = new Text("tom");
		//key 需要按照顺序来
		mWriter.append(new IntWritable(22), val);
		mWriter.append(new IntWritable(23), val);
		mWriter.append(new IntWritable(24), val);
		mWriter.close();
		System.out.println("over!!!!!!!!!!!!");
	}
	/**
	 * 读取mapfile
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void readFile() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		MapFile.Reader mReader = new MapFile.Reader( fs, "hdfs://s100:8020/user/ubuntu/mymap.map",conf);
		//key 需要按照顺序来
		IntWritable key = new IntWritable();
		Text val = new Text();
		while (mReader.next(key, val))
		{
		System.out.println("key:"+key+" value:"+val);
		}
		mReader.close();
		
		mReader.get(new IntWritable(22), val);
		System.out.println("val:"+val);
		System.out.println("over!!!!!!!!!!!!");
	}
}
