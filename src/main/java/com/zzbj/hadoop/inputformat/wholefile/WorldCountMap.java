package com.zzbj.hadoop.inputformat.wholefile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.zzbj.hadoop.mr.UtilHelper;

/****
 * 创建mapper类
 * 
 * @author zhuhuijun
 *
 */

public class WorldCountMap extends Mapper<NullWritable, BytesWritable, Text, IntWritable> {
	protected void map(NullWritable key, BytesWritable value,
			Mapper<NullWritable, BytesWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		byte[] data = value.copyBytes();
		Text text = new Text();
		IntWritable one = new IntWritable(1);

		String str = new String(data, "utf-8");
		String[] arr = str.replace("\r\n", " ").split(" ");
		for (String w : arr) {
			text.set(w);
			context.write(text, one);
		}
	}
}
