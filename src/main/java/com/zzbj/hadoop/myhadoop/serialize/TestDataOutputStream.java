package com.zzbj.hadoop.myhadoop.serialize;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import org.junit.Test;

public class TestDataOutputStream
{
	@Test
	public void testDataOutput() throws Exception
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeUTF("hello");
		dos.writeUTF("WORLD");
		dos.close();
		bos.close();

		DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
		String val = dataInputStream.readUTF();
		String val2 = dataInputStream.readUTF();
		System.out.println("val:" + val);
		System.out.println("val:" + val2);
		System.out.println("over!!!!!!!!!!");
	}
	@Test
	public void testDataOutput2() throws Exception
	{
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		DataOutputStream dos = new DataOutputStream(bos);
		dos.writeBytes("hello中");
		dos.close();
		bos.close();
		
		DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
		String val = dataInputStream.readUTF();
		System.out.println("val:" + val);
		System.out.println("over!!!!!!!!!!");
	}
}
