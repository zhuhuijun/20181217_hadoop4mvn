package com.zzbj.hadoop.seqfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class SeqFile {
	/***
	 * 
	 * @param args
	 */
	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		try {

			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://s100:8020");
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path("hdfs://s100:8020/user/ubuntu/data/seq/myseqfile.seq");
			Writer writer = SequenceFile.createWriter(fs, conf, path, IntWritable.class,
					Text.class);

			IntWritable key = new IntWritable();
			Text value = new Text();

			for (int i = 0; i < 100; i++) {
				key.set(i);
				value.set("name=tom" + i);
				writer.append(key, value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println("over!!!!!!");
	}
}
