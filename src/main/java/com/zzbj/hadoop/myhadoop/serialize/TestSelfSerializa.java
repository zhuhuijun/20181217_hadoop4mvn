package com.zzbj.hadoop.myhadoop.serialize;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;

import org.junit.Test;

public class TestSelfSerializa
{
	@Test
	public void serizlize() throws Exception
	{
		ByteArrayOutputStream baso = new ByteArrayOutputStream();
		// 包装流
		ObjectOutputStream oos = new ObjectOutputStream(baso);
		oos.writeObject(new Dog("dog"));
		oos.close();
		baso.close();
		System.out.println("length:" + baso.toByteArray().length);
		System.out.println("over!!!!!!!!!");

	}

	@Test
	public void serialize() throws Exception
	{
		ByteArrayOutputStream baso = new ByteArrayOutputStream();
		// 包装流
		DataOutputStream oos = new DataOutputStream(baso);
		Person person = new Person(123, "123", 123);
		Address add = new Address();
		add.setCity("city");
		add.setCountry("country");
		add.setProvince("province");
		person.setAddress(add);
		person.write(oos);
		oos.close();
		baso.close();
		System.out.println(baso.toByteArray().length);
		System.out.println("over!!!!!!!!");
	}
}
