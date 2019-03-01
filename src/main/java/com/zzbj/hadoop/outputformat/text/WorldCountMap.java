package com.zzbj.hadoop.outputformat.text;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/****
 * 创建mapper类
 * 
 * @author zhuhuijun
 *
 */

public class WorldCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Text text = new Text();
		IntWritable one = new IntWritable(1);

		String line = value.toString();
		String[] arr = line.split(" ");
		for (String w : arr) {
			text.set(w);
			context.write(text, one);
		}
	}
}
