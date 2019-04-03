package com.zzbj.hadoop.sort.secondary;

import java.io.IOException;
import java.net.InetAddress;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempReducer extends Reducer<CombineKey, NullWritable, CombineKey, NullWritable> {
	InetAddress localHost;

	protected void reduce(CombineKey keyin, Iterable<NullWritable> valuein,
			Reducer<CombineKey, NullWritable, CombineKey, NullWritable>.Context context)
			throws IOException, InterruptedException {
		context.write(keyin, NullWritable.get());
	}

}
