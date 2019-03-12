package com.zzbj.hadoop.sort.sample;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/****
 * 创建mapper类
 * 
 * @author zhuhuijun
 *
 */

public class MaxTempMap extends Mapper<IntWritable, Text, IntWritable, Text> {

	/***
	 * mapper
	 */
	protected void map(IntWritable key, Text value,
			Mapper<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		context.write(key, value);
	}

}
