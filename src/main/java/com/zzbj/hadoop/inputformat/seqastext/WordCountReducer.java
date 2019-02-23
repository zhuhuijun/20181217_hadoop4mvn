package com.zzbj.hadoop.inputformat.seqastext;

import com.zzbj.hadoop.mr.UtilHelper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	/***
	 * setup
	 */
	protected void setup(Context context)
			throws IOException, InterruptedException {
		context.getCounter("e", UtilHelper.GetGrp2("WCReduce.setup", this.hashCode())).increment(1);
	}

	protected void reduce(Text keyin//
			, Iterable<IntWritable> valuein//
			, Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable cIntWritable : valuein) {
			count = count+ cIntWritable.get();
		}
		context.write(keyin, new IntWritable(count));
	}

	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		context.getCounter("e", UtilHelper.GetGrp2("WCReducer.cleanup", this.hashCode()))
				.increment(1);
	}
}
