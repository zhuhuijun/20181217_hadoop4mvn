package com.zzbj.hadoop.inputformat.seqastext;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/****
 * 创建mapper类
 * 
 * @author zhuhuijun
 *
 */

public class WorldCountMap extends Mapper<Text, Text, Text, IntWritable> {
	protected void map(Text key, Text value, Mapper<Text, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		IntWritable val = new IntWritable(1);
		context.write(value, val);
		context.getCounter("m", "mapper.map." + key.toString()).increment(1);
	}
}
