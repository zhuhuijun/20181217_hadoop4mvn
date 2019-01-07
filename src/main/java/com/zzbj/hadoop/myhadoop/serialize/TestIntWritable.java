package com.zzbj.hadoop.myhadoop.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class TestIntWritable
{
	/***
	 * 序列化的过程
	 * 
	 * @throws Exception
	 */
	@Test
	public void serialize() throws Exception
	{
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputStream oos = new DataOutputStream(baos);
		IntWritable w = new IntWritable(255);
		w.write(oos);
		oos.close();
		baos.close();
		byte[] buf = baos.toByteArray();
		// 0 0 0 1 代表8个1
		System.out.println(buf.length);
		System.out.println("over!!!!!!!!!!!!!!");

		IntWritable i2 = new IntWritable();
		i2.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
		System.err.println(i2.get());
	}

	/**
	 * 进行比较大小
	 */
	@Test
	public void compare()
	{
		IntWritable i1 = new IntWritable(3);
		IntWritable i2 = new IntWritable(3);
		System.err.println(i1.compareTo(i2));
		System.out.println("over!!!!!!!!!");
	}
	/**
	 * 比较器进行大小的比较
	 */
	@Test
	public void compare2()
	{
		IntWritable.Comparator comparator = new IntWritable.Comparator();
		IntWritable i1 = new IntWritable(3);
		IntWritable i2 = new IntWritable(332);
		System.err.println(comparator.compare(i1, i2));
		System.out.println("over!!!!!!!!!");
	}

}

