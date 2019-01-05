package com.zzbj.hadoop.myhadoop.serialize;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

public class TestJavaSerializa
{
	/**
	 * 串行化的方式java并不高 需要考虑1、带宽的占用
	 * 
	 * @param
	 * @throws Exception
	 */
	@Test
	public void serizlize() throws Exception
	{
		ByteArrayOutputStream baso = new ByteArrayOutputStream();
		// 包装流
		ObjectOutputStream oos = new ObjectOutputStream(baso);
		oos.writeObject(new Dog("dog"));
		oos.close();
		baso.close();
		System.out.println("length:"+baso.toByteArray().length);
		System.out.println("over!!!!!!!!!");

	}
}
