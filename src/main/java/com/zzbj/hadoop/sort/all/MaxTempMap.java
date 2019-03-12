package com.zzbj.hadoop.sort.all;

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

public class MaxTempMap extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	private static final int MISSING = 9999;

	/***
	 * 
	 */
	protected void map(LongWritable key//
			, Text value//
			, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		if (line.charAt(87) == '+') {
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else {
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		if (airTemperature != MISSING && quality.matches("[01459]")) {
			context.write(new IntWritable(Integer.parseInt(year)), new IntWritable(airTemperature));
		}
		context.getCounter("m", "maxmapper.max").increment(1);
	}

}
