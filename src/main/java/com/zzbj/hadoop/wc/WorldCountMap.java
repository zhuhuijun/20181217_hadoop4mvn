package com.zzbj.hadoop.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zzbj.hadoop.mr.UtilHelper;

/****
 * 创建mapper类
 * 
 * @author zhuhuijun
 *
 */

public class WorldCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MISSING = 9999;

	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		context.getCounter("m", UtilHelper.GetGrp2("WcMapper.setup", this.hashCode())).increment(1);
	}

	/***
	 * 
	 */
	protected void map(LongWritable key//
			, Text value//
			, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split(" ");
		for (String tmp : arr) {
			context.write(new Text(tmp), new IntWritable(1));
		}
		context.getCounter("m", UtilHelper.GetGrp2("WcMapper.map", this.hashCode())).increment(1);
	}

	protected void cleanup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		context.getCounter("m", UtilHelper.GetGrp2("WcMapper.cleanup", this.hashCode()))
				.increment(1);
	}

}
