package com.zzbj.hadoop.myhadoop.serialize;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/***
 * 自定义数据类型实现writable接口
 * @author zhuhuijun
 *
 */
public class Person implements Writable
{
	private int id;
	private String name;
	private int age;
	private Address address;
	
	public Address getAddress()
	{
		return address;
	}

	public void setAddress(Address address)
	{
		this.address = address;
	}

	public Person()
	{
		super();
	}

	public Person(int id, String name, int age)
	{
		super();
		this.id = id;
		this.name = name;
		this.age = age;
	}

	public int getId()
	{
		return id;
	}

	public void setId(int id)
	{
		this.id = id;
	}

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public int getAge()
	{
		return age;
	}

	public void setAge(int age)
	{
		this.age = age;
	}

	public void write(DataOutput out) throws IOException
	{
		out.writeInt(id);
		out.writeUTF(name);
		out.writeInt(age);
		if (null == address) {
			address = new Address();
		}
		out.writeUTF(address.getProvince());
		out.writeUTF(address.getCity());
		out.writeUTF(address.getCountry());
	}

	public void readFields(DataInput in) throws IOException
	{
		this.id = in.readInt();
		this.name = in.readUTF();
		this.age = in.readInt();
		if (null == address) {
			address = new Address();
		}
		address.setProvince(in.readUTF());
		address.setCity(in.readUTF());
		address.setCountry(in.readUTF());
		this.address=address;
	}
}
