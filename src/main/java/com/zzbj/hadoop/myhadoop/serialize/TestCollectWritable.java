package com.zzbj.hadoop.myhadoop.serialize;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

/**
 * 测试Arraywritable
 * 
 * @author zhuhuijun
 *
 */
public class TestCollectWritable
{
//测试串行化的方法我的线程没问题
	@Test
	public void testArrayWritable() throws Exception, IOException
	{
		// 创建writable的集合

		Writable[] data = new Text[10];
		for (int a = 0; a < 10; a++)
		{
			data[a] = new Text("tom" + a);
		}
		// 构造arraywritable data
		ArrayWritable aw = new ArrayWritable(Text.class);
		aw.set(data);
		// 数组对象写入
		aw.write(new DataOutputStream(new FileOutputStream("d:/writ.dat")));

		System.out.println("over!!!!!!!");
	}

	/****
	 * map的writable
	 * 
	 * @throws Exception
	 * @throws IOException
	 */
	@Test
	public void mapWriteable() throws Exception, IOException
	{
		MapWritable mapWritable = new MapWritable();
		for (int a = 0; a < 10; a++)
		{
			mapWritable.put(new IntWritable(a), new Text("tom" + a));
		}
		mapWritable.write(new DataOutputStream(new FileOutputStream("d://map.data")));
		System.out.println("over!!!!!!!");
	}
}
