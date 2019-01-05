package com.zzbj.hadoop.myhadoop.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.sun.org.apache.xpath.internal.operations.String;

public class TestText
{
	@Test
	public void testText() throws Exception
	{
		Text text = new Text("h中elloworld");
		int charAt = text.charAt(0);
		System.out.println(charAt);
		System.out.println("getleng=" + text.getLength());
		int find = text.find("o", 3);
		System.out.println("find===" + find);

		java.lang.String decode = text.decode(text.getBytes(), 0, text.getLength());
		System.out.println("decode=" + decode);
		System.out.println("over!!!!!!");
	}

	/**
	 * 穿行和反串行
	 * 
	 * @throws Exception
	 */
	@Test
	public void serializeAndDeSerialize() throws Exception
	{
		Long start = System.nanoTime();
		FileOutputStream fos = new FileOutputStream("d:/data.dat");
		DataOutputStream dos = new DataOutputStream(fos);
		IntWritable iw = new IntWritable(190); //4
		iw.write(dos);
		LongWritable lw = new LongWritable(4232L); //8
		lw.write(dos);

		Text text =new Text("hello"); //5
		text.write(dos);
		
		text.set("world"); //5
		text.write(dos);;
		
		dos.close();
		fos.close();
		
		System.out.println("over!!!!!!");
		System.out.println(System.nanoTime()-start);
	}
	/**
	 * 
	 * @throws Exception
	 */
	@Test
	public void sDeSerialize() throws Exception
	{
		FileInputStream fos = new FileInputStream("d:/data.dat");
		byte[] bytes =  new byte[fos.available()];
		int read = fos.read(bytes);
		fos.close();
		
		DataInputStream dos = new DataInputStream(new ByteArrayInputStream(bytes));
		IntWritable iw = new IntWritable(); //4
		iw.readFields(dos);
		System.out.println(iw.get());
		
		LongWritable lw = new LongWritable(4232L); //8
		lw.readFields(dos);
		System.out.println(lw.get());
		
		Text text =new Text(); //5
		text.readFields(dos);
		System.out.println(text.toString());
		
		text.readFields(dos);
		System.out.println(text.toString());
		dos.close();
		System.out.println("over!!!!!!");
	}
	/***
	 * java的序列化占用的大小
	 * @throws Exception
	 */
	@Test
	public void javaserialize() throws Exception
	{
		Long start = System.nanoTime();
		FileOutputStream baso = new FileOutputStream("d://java.data");
		// 包装流
		ObjectOutputStream oos = new ObjectOutputStream(baso);
		oos.writeObject(new Integer(190));
		oos.writeObject(new Long(4232L));
		oos.writeObject(new StringBuilder("hello"));
		oos.writeObject(new StringBuilder("world"));
		oos.close();
		baso.close();
		System.out.println("over!!!!!!");
		System.out.println(System.nanoTime()-start);
	}
}
