package com.zzbj.hadoop.myhadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.ArrayFile.Reader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SetFile;
import org.apache.hadoop.io.SetFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestSetFile
{
	@SuppressWarnings("deprecation")
	@Test
	public void writeFile() throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://s100:8020");
		FileSystem fs = FileSystem.get(conf);
		Writer mWriter = new SetFile.Writer(conf, fs, "hdfs://s100:8020/user/ubuntu/mysetfile.map", IntWritable.class,
				CompressionType.NONE);
		for (int j = 0; j < 1200; j++)
		{
			mWriter.append(new IntWritable(j));
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
		Reader mReader = new ArrayFile.Reader(fs, "hdfs://s100:8020/user/ubuntu/mysetfile.map", conf);
		// key 需要按照顺序来
		IntWritable key = new IntWritable();
		Text val = new Text();
		while ((val = (Text) mReader.next(val)) != null)
		{
			System.out.println(" value:" + val);
		}
		mReader.close();

		System.out.println("val:" + val);
		System.out.println("over!!!!!!!!!!!!");
	}
}
