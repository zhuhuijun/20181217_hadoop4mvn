package com.zzbj.hadoop.wc2;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.zzbj.hadoop.mr.UtilHelper;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/***
	 * setup
	 */
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		context.getCounter("e", UtilHelper.GetGrp2("WCReduce.setup", this.hashCode())).increment(1);
	}

	protected void reduce(Text keyin//
			, Iterable<IntWritable> valuein//
			, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable cIntWritable : valuein) {
			count = count+ cIntWritable.get();
		}
		context.write(keyin, new IntWritable(count));
	}

	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		context.getCounter("e", UtilHelper.GetGrp2("WCReducer.cleanup", this.hashCode()))
				.increment(1);
	}
}
