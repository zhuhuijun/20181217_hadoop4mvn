package com.zzbj.hadoop.sort.sample;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/***
 * Reducer
 * 
 * @author zhuhuijun
 *
 */
public class MaxTempReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	protected void reduce(IntWritable keyin, Iterable<Text> valuein,
			Reducer<IntWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		context.write(keyin, valuein.iterator().next());
	}

}
