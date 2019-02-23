package com.zzbj.hadoop.inputformat.seqfile;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/****
 * 创建mapper类
 * 
 * @author zhuhuijun
 *
 */

public class WorldCountMap extends Mapper<IntWritable, Text, Text, IntWritable> {
	protected void map(IntWritable key, Text value, Mapper<IntWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		IntWritable val = new IntWritable(1);
		context.write(value, val);
		context.getCounter("m", "mapper.map." + key.toString()).increment(1);
	}
}
