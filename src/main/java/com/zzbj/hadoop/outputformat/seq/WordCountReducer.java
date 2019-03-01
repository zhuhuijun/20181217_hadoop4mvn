package com.zzbj.hadoop.outputformat.seq;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {


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

}
