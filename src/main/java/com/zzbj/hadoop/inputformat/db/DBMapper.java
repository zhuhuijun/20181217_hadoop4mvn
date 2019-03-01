package com.zzbj.hadoop.inputformat.db;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/***
 * mapper
 *
 * @author zhuhuijun
 *
 */
public class DBMapper extends Mapper<LongWritable, HadoopUserDBWritable, Text, Text> {
	protected void map(LongWritable key, HadoopUserDBWritable value, Context context)
			throws IOException, InterruptedException {
		int id = value.getId();
		String name = value.getName();
		int age = value.getAge();
		context.write(new Text(id + ""), new Text(name + "," + age));
	}
}
