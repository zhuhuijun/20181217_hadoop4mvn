package com.zzbj.hadoop.inputformat.nline;

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

public class WorldCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] arr = value.toString().split(" ");
		Text onekey = new Text();
		IntWritable val = new IntWritable(1);
		for (String t : arr) {
			onekey.set(t);
			context.write(onekey, val);
		}
		context.getCounter("m", "mapper.map." + key.toString()).increment(1);
	}
}
