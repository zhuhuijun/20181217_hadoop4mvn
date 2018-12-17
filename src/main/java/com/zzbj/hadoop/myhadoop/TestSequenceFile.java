package com.zzbj.hadoop.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class TestSequenceFile
{
	public static void main(String[] args) throws Exception
	{
		// Write();
//		WriteWithSync();
		InWriteNoCompression();
	}

	/**
	 * 写入序列文件
	 * 
	 * @throws Exception
	 */
	public static void Write() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/ubuntu/mysequencefile.seq");
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
	 * 插入同步点
	 * 
	 * @throws Exception
	 */
	public static void WriteWithSync() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/ubuntu/mysequencefile.seq");
		Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class, Text.class);
		IntWritable key = new IntWritable();
		Text val = new Text();
		for (int i = 0; i < 100; i++)
		{
			key.set(i);
			val.set("tom" + i);
			if (i % 5 == 0)
			{
				writer.sync();
			}
			writer.append(key, val);
		}
		writer.close();
		System.out.println("over!!!");

	}

	@SuppressWarnings("deprecation")
	public static void InWriteNoCompression() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/ubuntu/mysequencefile.seq");
		// Writer writer = SequenceFile.createWriter(fs, conf, path,
		// IntWritable.class, Text.class);
//		Writer writer = SequenceFile.createWriter(conf, fs.create(path), IntWritable.class, Text.class,
//				CompressionType.NONE, null);
		BZip2Codec codec = ReflectionUtils.newInstance(BZip2Codec.class, conf);
		Writer writer = SequenceFile.createWriter(conf, fs.create(path), IntWritable.class, Text.class,
				CompressionType.BLOCK, codec);
		IntWritable key = new IntWritable();
		Text val = new Text();
		for (int i = 0; i < 100000; i++)
		{
			key.set(i);
			val.set("tom" + i);
			if (i % 5 == 0)
			{
				writer.sync();
			}
			writer.append(key, val);
		}
		writer.close();
		System.out.println("over!!!");

	}

	/**
	 * 读文件
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void Read() throws Exception
	{
		System.out.println(System.currentTimeMillis());
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/ubuntu/mysequencefile.seq");
		Reader reader = new SequenceFile.Reader(fs, path, conf);
		CompressionType type = reader.getCompressionType();
		System.out.println("compression:" + type);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		long position = reader.getPosition();
		while (reader.next(key, value))
		{
			System.out.printf("position:%d,key:%s,value:%s\n", position, key, value);
			position = reader.getPosition();
		}
		IOUtils.closeStream(reader);
		System.out.println("read over!!!");
		System.out.println(System.currentTimeMillis());

	}

	/**
	 * 跳过
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void Seek() throws Exception
	{
		System.out.println(System.currentTimeMillis());
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/ubuntu/mysequencefile.seq");
		Reader reader = new SequenceFile.Reader(fs, path, conf);
		CompressionType type = reader.getCompressionType();
		System.out.println("compression:" + type);
		Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
		reader.seek(206);
		reader.next(key, value);
		System.out.println(key + ":" + value);
		long position = reader.getPosition();
		while (reader.next(key, value))
		{
			System.out.printf("position:%d,key:%s,value:%s\n", position, key, value);
			position = reader.getPosition();
		}
		IOUtils.closeStream(reader);
		System.out.println("read over!!!");
		System.out.println(System.currentTimeMillis());

	}

	/**
	 * 读取文件的同步点
	 * 
	 * @throws Exception
	 */
	@SuppressWarnings("deprecation")
	@Test
	public void sync() throws Exception
	{
		System.out.println(System.currentTimeMillis());
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("hdfs://s100:8020/user/ubuntu/mysequencefile.seq");
		Reader reader = new SequenceFile.Reader(fs, path, conf);
		CompressionType type = reader.getCompressionType();
		System.out.println("compression:" + type);
		// Writable key = (Writable)
		// ReflectionUtils.newInstance(reader.getKeyClass(), conf);
		// Writable value = (Writable)
		// ReflectionUtils.newInstance(reader.getValueClass(),
		// conf);
		IntWritable key = new IntWritable();
		Text value = new Text();
		int syncpos = 418;
		// 定位到下一个同步点
		reader.sync(syncpos);
		long pos = reader.getPosition();

		// System.out.println(syncpos+"sync
		// :"+reader.getPosition());
		reader.next(key, value);
		System.out.println(syncpos + " :" + pos + " ,key:" + key + ",value:" + value);
		while (reader.next(key, value))
		{
			System.out.printf("position:%d,key:%s,value:%s\n", pos, key, value);
			pos = reader.getPosition();
		}
		IOUtils.closeStream(reader);
		System.out.println("read over!!!");
		System.out.println(System.currentTimeMillis());

	}
}
