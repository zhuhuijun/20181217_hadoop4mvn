package com.zzbj.hadoop.mr;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	protected void reduce(Text keyin//
			, Iterable<IntWritable> valuein//
			, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		InetAddress localHost = InetAddress.getLocalHost();
		System.out.println("reduce>>>>" + localHost.getHostAddress());
		int maxValue = Integer.MIN_VALUE;
		for (IntWritable value : valuein) {
			maxValue = Math.max(maxValue, value.get());
		}
		context.write(keyin, new IntWritable(maxValue));
	}

}
