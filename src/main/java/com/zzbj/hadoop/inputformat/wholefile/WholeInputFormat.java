package com.zzbj.hadoop.inputformat.wholefile;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/***
 * 自定义输入格式 将整个文件作为一条Record
 * 
 * @author zhuhuijun
 *
 */
public class WholeInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

	public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException, InterruptedException {
		WholeRecordReader reader = new WholeRecordReader();
		reader.initialize(split, context);
		context.getCounter("in", "wholeinput.wholerecordreader").increment(1);
		return reader;
	}

	/***
	 * 不能切割
	 */
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

}
