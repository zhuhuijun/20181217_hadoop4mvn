package com.zzbj.hadoop.outputformat.db;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, WordDBWritable, NullWritable> {

	protected void reduce(Text keyin//
			, Iterable<IntWritable> valuein//
			, Reducer<Text, IntWritable, WordDBWritable, NullWritable>.Context context)
			throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable cIntWritable : valuein) {
			count = count + cIntWritable.get();
		}
		WordDBWritable dbkey = new WordDBWritable();
		dbkey.setWord(keyin.toString());
		dbkey.setNum(count);
		context.write(dbkey, NullWritable.get());
	}

}
