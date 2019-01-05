package com.zzbj.hadoop.mr;

import java.io.IOException;

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

public class MaxTempMap extends Mapper<LongWritable, Text, Text, IntWritable>
{
	private static final int MISSING = 9999;

	@Override
	protected void map(LongWritable key//
			, Text value//
			, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException
	{
		System.out.println("111111111111111");
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature;
		if (line.charAt(87) == '+')
		{
			airTemperature = Integer.parseInt(line.substring(88, 92));
		} else
		{
			airTemperature = Integer.parseInt(line.substring(87, 92));
		}
		String quality = line.substring(92, 93);
		if (airTemperature != MISSING && quality.matches("[01459]"))
		{
			context.write(new Text(year), new IntWritable(airTemperature));
		}

	}

}
