package com.zzbj.hadoop.myhadoop.serialize;

import java.io.Serializable;

public class Dog implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1202848341884523401L;
	private String name;

	public String getName()
	{
		return name;
	}

	public void setName(String name)
	{
		this.name = name;
	}

	public Dog(String name)
	{
		super();
		this.name = name;
	}




}
