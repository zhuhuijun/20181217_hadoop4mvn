package com.zzbj.hadoop.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.junit.Test;

public class TestMapFile
{
	public static void main(String[] args)
	{

	}

	@SuppressWarnings("deprecation")
	@Test
	public void writeFile() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		MapFile.Writer mWriter = new MapFile.Writer(conf, fs, "hdfs://s100:8020/user/ubuntu/mymap.map",
				IntWritable.class, Text.class);
		Text val = new Text();
		int indexInterval = mWriter.getIndexInterval();
		System.out.println("default interval:" + indexInterval);
		// 设置索引间隔
		mWriter.setIndexInterval(256);
		// key 需要按照顺序来
		for (int j = 0; j < 2000; j += 3)
		{
			val.set("tom" + j);
			mWriter.append(new IntWritable(j), val);
		}
		mWriter.close();
		System.out.println("over!!!!!!!!!!!!");
	}

	/**
	 * 读取mapfile
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
		MapFile.Reader mReader = new MapFile.Reader(fs, "hdfs://s100:8020/user/ubuntu/mymap.map", conf);
		// key 需要按照顺序来
		IntWritable key = new IntWritable();
		Text val = new Text();
		while (mReader.next(key, val))
		{
			System.out.println("key:" + key + " value:" + val);
		}
		mReader.close();

		mReader.get(new IntWritable(22), val);
		System.out.println("val:" + val);
		System.out.println("over!!!!!!!!!!!!");
	}

	/***
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void readlateFile() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		MapFile.Reader mReader = new MapFile.Reader(fs, "hdfs://s100:8020/user/ubuntu/mymap.map", conf);
		// key 需要按照顺序来
		IntWritable key = new IntWritable();
		Text val = new Text();
		mReader.getClosest(new IntWritable(22), val, true);
		mReader.close();
		System.out.println("val:" + val);
		System.out.println("over!!!!!!!!!!!!");
	}
	/***
	 * 
	 * @throws Exception
	 */
	@Test
	public  void preparwWrite() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/ubuntu/mysequencefile2.seq");
		Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
		for (int i = 0; i < 100; i++)
		{
			IntWritable key = new IntWritable(i);
			Text val = new Text("tom" + i);
			writer.append(key, val);
		}
		writer.close();
		System.out.println("over!!!");

	}
	/**
	 * sequencefile to mapfile
	 * 
	 * @throws Exception
	 */
	@Test
	public void seq2mapfile() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		// 序列文件
		Path path = new Path("hdfs://s100:8020/user/ubuntu/mysequencefile2.seq");
		Path mappath = new Path("hdfs://s100:8020/user/ubuntu/mymap2.map");
		long fix = MapFile.fix(fs, path, IntWritable.class, Text.class, false, conf);
		System.out.println("over!!!!!!!!!!");
	}
}
